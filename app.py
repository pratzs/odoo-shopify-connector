import os
import hmac
import hashlib
import base64
import json
import threading
import schedule
import time
import shopify
from flask import Flask, request, jsonify, render_template, redirect, url_for, session
from models import db, ProductMap, SyncLog, AppSetting, CustomerMap, Shop
from odoo_client import OdooClient
import requests
from datetime import datetime, timedelta

app = Flask(__name__)
app.secret_key = os.urandom(24)

# --- PUBLIC APP CONFIG ---
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_SECRET = os.getenv('SHOPIFY_SECRET')
APP_URL = os.getenv('APP_URL')

# --- DATABASE CONFIG ---
database_url = os.getenv('DATABASE_URL', 'sqlite:///local.db')
if database_url and database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql+pg8000://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
shopify.Session.setup(api_key=SHOPIFY_API_KEY, secret=SHOPIFY_SECRET)

# --- GLOBAL LOCKS ---
order_processing_lock = threading.Lock()
active_processing_ids = set()


# --- CRITICAL FIX: Disable Strict Scope Validation ---
# This forces the library to accept new scopes like 'customer_write_customers'
from shopify import api_access
api_access.ApiAccess = lambda x: x 

# ... app = Flask(__name__) starts here ...
# --- HELPERS ---
def get_shop_config(shop_id, key, default=None):
    try:
        with app.app_context():
            setting = AppSetting.query.filter_by(shop_id=shop_id, key=key).first()
            if not setting: return default
            try: return json.loads(setting.value)
            except: return setting.value
    except: return default

def set_shop_config(shop_id, key, value):
    try:
        with app.app_context():
            setting = AppSetting.query.filter_by(shop_id=shop_id, key=key).first()
            if not setting:
                setting = AppSetting(shop_id=shop_id, key=key)
                db.session.add(setting)
            setting.value = json.dumps(value)
            db.session.commit()
            return True
    except:
        db.session.rollback()
        return False

def get_odoo_connection(shop):
    if not shop.odoo_url or not shop.odoo_password: return None
    try:
        return OdooClient(shop.odoo_url, shop.odoo_db, shop.odoo_username, shop.odoo_password)
    except Exception as e:
        print(f"Odoo Connect Error: {e}")
        return None

def verify_webhook(data, hmac_header):
    if not SHOPIFY_SECRET: return True
    digest = hmac.new(SHOPIFY_SECRET.encode('utf-8'), data, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), hmac_header)

def log_event(shop_id, entity, status, message):
    try:
        with app.app_context():
            log = SyncLog(shop_id=shop_id, entity=entity, status=status, message=message, timestamp=datetime.utcnow())
            db.session.add(log)
            db.session.commit()
    except: db.session.rollback()

def extract_id(res):
    if isinstance(res, list) and len(res) > 0: return res[0]
    return res

# --- GRAPHQL HELPERS (Context Aware) ---
def find_shopify_product_by_sku(sku):
    query = '{ productVariants(first: 1, query: "sku:%s") { edges { node { product { legacyResourceId } } } } }' % sku
    try:
        client = shopify.GraphQL()
        result = client.execute(query)
        data = json.loads(result)
        edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
        if edges: return edges[0]['node']['product']['legacyResourceId']
    except: pass
    return None

def get_shopify_variant_inv_by_sku(sku):
    query = '{ productVariants(first: 1, query: "sku:%s") { edges { node { legacyResourceId inventoryItem { legacyResourceId } inventoryQuantity } } } }' % sku
    try:
        client = shopify.GraphQL()
        result = client.execute(query)
        data = json.loads(result)
        edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
        if edges:
            node = edges[0]['node']
            return {
                'variant_id': node['legacyResourceId'],
                'inventory_item_id': node['inventoryItem']['legacyResourceId'],
                'qty': node['inventoryQuantity']
            }
    except: pass
    return None

# --- CORE LOGIC: ORDERS ---
def process_order_data(data, shop, odoo):
    shopify_id = str(data.get('id', ''))
    if shopify_id in active_processing_ids: return False
    active_processing_ids.add(shopify_id)

    try:
        email = data.get('email') or data.get('contact_email')
        client_ref = f"ONLINE_{data.get('name')}"
        company_id = shop.odoo_company_id
        
        existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
        existing_order_id = existing_ids[0] if existing_ids else None

        partner = odoo.search_partner_by_email(email)
        if not partner:
            cust = data.get('customer', {})
            addr = data.get('billing_address') or {}
            vat = None
            for a in data.get('note_attributes', []):
                if a.get('name', '').lower() in ['vat', 'vat_number']: vat = a.get('value')

            vals = {
                'name': addr.get('company') or f"{cust.get('first_name')} {cust.get('last_name')}",
                'email': email, 'phone': cust.get('phone'),
                'street': addr.get('address1'), 'city': addr.get('city'),
                'zip': addr.get('zip'), 'country_code': addr.get('country_code'),
                'vat': vat, 'is_company': True, 'company_type': 'company'
            }
            if company_id: vals['company_id'] = int(company_id)
            partner_id = odoo.create_partner(vals)
            partner = {'id': partner_id, 'name': vals['name']}
            
            if shopify_id and cust.get('id'):
                with app.app_context():
                    db.session.add(CustomerMap(shopify_customer_id=str(cust.get('id')), shop_id=shop.id, odoo_partner_id=partner_id, email=email))
                    db.session.commit()
        
        partner_id = extract_id(partner['parent_id'][0] if partner.get('parent_id') else partner['id'])

        def get_child(addr_data, type_val):
            if not addr_data: return partner_id
            name = addr_data.get('name') or partner['name']
            if name == partner['name']: name = f"{name} ({type_val.title()})"
            return odoo.find_or_create_child_address(partner_id, {
                'name': name, 'street': addr_data.get('address1'), 'city': addr_data.get('city'),
                'zip': addr_data.get('zip'), 'country_code': addr_data.get('country_code'),
                'phone': addr_data.get('phone'), 'email': email
            }, type_val)

        invoice_id = get_child(data.get('billing_address'), 'invoice')
        shipping_id = get_child(data.get('shipping_address'), 'delivery')
        user_id = odoo.get_partner_salesperson(partner_id) or odoo.uid

        lines = []
        for item in data.get('line_items', []):
            sku = item.get('sku')
            if not sku: continue
            pid = odoo.search_product_by_sku(sku, company_id)
            if not pid:
                try:
                    odoo.create_product({'name': item['name'], 'default_code': sku, 'list_price': float(item['price']), 'type': 'product', 'company_id': int(company_id) if company_id else False})
                    pid = odoo.search_product_by_sku(sku, company_id)
                except: pass
            
            if pid:
                price, qty = float(item['price']), int(item['quantity'])
                disc = float(item.get('total_discount', 0))
                pct = (disc / (price * qty)) * 100 if price > 0 else 0.0
                lines.append((0,0, {'product_id': pid, 'product_uom_qty': qty, 'price_unit': price, 'name': item['name'], 'discount': pct}))

        for ship in data.get('shipping_lines', []):
            cost = float(ship.get('price', 0.0))
            if cost >= 0:
                spid = odoo.search_product_by_sku("SHIP_FEE", company_id) or odoo.search_product_by_name("Shopify Shipping", company_id)
                if not spid:
                    try: 
                        odoo.create_service_product("Shopify Shipping", company_id)
                        spid = odoo.search_product_by_name("Shopify Shipping", company_id)
                    except: pass
                if spid: lines.append((0,0, {'product_id': spid, 'product_uom_qty': 1, 'price_unit': cost, 'name': ship['title'], 'discount': 0.0}))

        if not lines: return False

        vals = {
            'partner_id': partner_id, 'partner_invoice_id': invoice_id, 'partner_shipping_id': shipping_id,
            'order_line': lines, 'user_id': user_id, 'note': f"Gateway: {data.get('gateway')}"
        }

        if existing_order_id:
            info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'read', [[existing_order_id]], {'fields': ['state']})
            if info and info[0]['state'] not in ['done', 'cancel']:
                vals['order_line'] = [(5,0,0)] + lines
                odoo.update_sale_order(existing_order_id, vals)
                log_event(shop.id, 'Order', 'Success', f"Updated {client_ref}")
        else:
            vals['name'] = client_ref
            vals['client_order_ref'] = client_ref
            vals['state'] = 'draft'
            if company_id: vals['company_id'] = int(company_id)
            odoo.create_sale_order(vals, context={'manual_price': True})
            log_event(shop.id, 'Order', 'Success', f"Created {client_ref}")

        return True
    except Exception as e:
        log_event(shop.id, 'Order', 'Error', str(e))
        return False
    finally:
        active_processing_ids.discard(shopify_id)

# --- CORE LOGIC: PRODUCTS (Webhook) ---
def process_product_data(data, shop, odoo):
    """Handles Shopify Product Webhooks (Update Only)."""
    ptype = data.get('product_type', '')
    cat_id = None
    if ptype:
        try:
            ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'search', [[['name', '=', ptype]]])
            cat_id = ids[0] if ids else odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'create', [{'name': ptype}])
        except: pass

    for v in data.get('variants', []):
        sku = v.get('sku')
        if not sku: continue
        pid = odoo.search_product_by_sku(sku, shop.odoo_company_id)
        if pid and cat_id:
            try:
                curr = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'read', [[pid]], {'fields': ['public_categ_ids']})
                if cat_id not in curr[0].get('public_categ_ids', []):
                    odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'write', [[pid], {'public_categ_ids': [(4, cat_id)]}])
            except: pass

            # --- TASKS (Multi-Tenant Logic) ---

def task_sync_products_master(shop, odoo):
    """Odoo -> Shopify Product Sync"""
    c_id = shop.odoo_company_id
    prods = odoo.get_all_products(c_id)
    active_skus = set()
    
    # Settings
    s_title = get_shop_config(shop.id, 'prod_sync_title', True)
    s_desc = get_shop_config(shop.id, 'prod_sync_desc', True)
    s_price = get_shop_config(shop.id, 'prod_sync_price', True)
    
    synced = 0
    with shopify.Session.temp(shop.shop_url, '2024-01', shop.access_token):
        for p in prods:
            sku = p.get('default_code')
            if not sku: continue
            
            # Archive Check
            if not p.get('active', True):
                sid = find_shopify_product_by_sku(sku)
                if sid:
                    try: 
                        sp = shopify.Product.find(sid)
                        if sp.status != 'archived': sp.status='archived'; sp.save()
                    except: pass
                continue

            active_skus.add(sku)
            sid = find_shopify_product_by_sku(sku)
            try:
                sp = shopify.Product.find(sid) if sid else shopify.Product()
                chg = False
                
                if s_title and sp.title != p['name']: sp.title = p['name']; chg=True
                if s_desc and (sp.body_html or '') != (p.get('description_sale') or ''): sp.body_html = p.get('description_sale'); chg=True
                if chg or not sid: sp.save()
                
                if not sid: sp = shopify.Product.find(sp.id)
                var = sp.variants[0] if sp.variants else shopify.Variant(prefix_options={'product_id': sp.id})
                
                vchg = False
                if var.sku != sku: var.sku = sku; vchg=True
                if s_price and var.price != str(p['list_price']): var.price = str(p['list_price']); vchg=True
                
                if vchg: var.save()
                synced += 1
            except Exception as e:
                log_event(shop.id, 'Product', 'Error', f"Failed {sku}: {e}")
                
    log_event(shop.id, 'Product Sync', 'Success', f"Synced {synced} products")

def task_sync_customers_master(shop, odoo):
    """Odoo -> Shopify Customer Sync"""
    odoo_custs = odoo.get_changed_customers("2000-01-01 00:00:00", shop.odoo_company_id)
    synced = 0
    
    with shopify.Session.temp(shop.shop_url, '2024-01', shop.access_token):
        for p in odoo_custs:
            email = p.get('email')
            if not email or "@" not in email: continue
            try:
                sc = shopify.Customer.search(query=f"email:{email}")
                c = sc[0] if sc else shopify.Customer()
                c.email = email
                c.first_name = p.get('name', '').split(' ')[0]
                c.last_name = ' '.join(p.get('name', '').split(' ')[1:]) or 'Customer'
                
                metas = []
                if p.get('vat'): metas.append(shopify.Metafield({'key': 'vat', 'value': p['vat'], 'type': 'single_line_text_field', 'namespace': 'custom'}))
                
                if metas: c.metafields = metas
                c.save()
                synced += 1
            except: pass
    log_event(shop.id, 'Customer Sync', 'Success', f"Synced {synced} customers")

def task_sync_inventory(shop, odoo):
    """Inventory Sync using Stock Moves"""
    fld = get_shop_config(shop.id, 'inventory_field', 'qty_available')
    locs = get_shop_config(shop.id, 'inventory_locations', [])
    sloc = get_shop_config(shop.id, 'shopify_location_id', os.getenv('SHOPIFY_WAREHOUSE_ID'))
    
    lookback = datetime.utcnow() - timedelta(minutes=35)
    try:
        pids = odoo.get_product_ids_with_recent_stock_moves(str(lookback), shop.odoo_company_id)
        if not pids: return
        
        with shopify.Session.temp(shop.shop_url, '2024-01', shop.access_token):
            updates = 0
            for pid in pids:
                total = int(odoo.get_total_qty_for_locations(pid, locs, fld))
                pdata = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'read', [pid], {'fields': ['default_code']})
                sku = pdata[0].get('default_code')
                if not sku: continue
                
                inv = get_shopify_variant_inv_by_sku(sku)
                if inv and int(inv['qty']) != total and sloc:
                    shopify.InventoryLevel.set(location_id=int(sloc), inventory_item_id=inv['inventory_item_id'], available=total)
                    updates += 1
                    log_event(shop.id, 'Inventory', 'Info', f"Updated {sku}: {total}")
            
            if updates: log_event(shop.id, 'Inventory', 'Success', f"Synced {updates} items")
    except Exception as e:
        print(f"Inventory Error {shop.shop_url}: {e}")

# --- MASTER SCHEDULER ---
def run_scheduler_jobs():
    with app.app_context():
        shops = Shop.query.filter(Shop.access_token != None).all()
        for shop in shops:
            odoo = get_odoo_connection(shop)
            if not odoo: continue
            
            # Run tasks in separate threads to avoid blocking
            threading.Thread(target=task_sync_inventory, args=(shop, odoo)).start()
            # You can add the daily tasks here with a check for time

def run_schedule():
    schedule.every(30).minutes.do(run_scheduler_jobs)
    while True:
        schedule.run_pending()
        time.sleep(1)

t = threading.Thread(target=run_schedule, daemon=True)
t.start()

# --- ROUTES ---

@app.route('/')
def index():
    shop_url = request.args.get('shop')
    if shop_url:
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if shop and shop.access_token: return render_template('dashboard.html', shop=shop)
        return redirect(url_for('auth', shop=shop_url))
    return "Please install via Shopify."

@app.route('/shopify/auth')
def auth():
    shop_url = request.args.get('shop')
    scopes = ['read_products', 'write_products', 'read_orders', 'write_orders', 'read_customers', 'write_customers', 'read_inventory', 'write_inventory']
    session = shopify.Session(shop_url, '2024-01')
    return redirect(session.create_permission_url(scopes, url_for('callback', _external=True)))

@app.route('/shopify/callback')
def callback():
    shop_url = request.args.get('shop')
    session = shopify.Session(shop_url, '2024-01')
    token = session.request_token(request.args)
    
    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop:
        shop = Shop(shop_url=shop_url)
        db.session.add(shop)
    shop.access_token = token
    shop.is_active = True
    db.session.commit()
    
    # Init Webhooks
    with shopify.Session.temp(shop_url, '2024-01', token):
        hooks = [
            {'topic': 'orders/updated', 'address': f'{APP_URL}/webhook/orders/updated'},
            {'topic': 'products/update', 'address': f'{APP_URL}/webhook/products/update'},
            {'topic': 'app/uninstalled', 'address': f'{APP_URL}/webhook/app/uninstalled'}
        ]
        for h in hooks:
            try:
                w = shopify.Webhook()
                w.topic = h['topic']; w.address = h['address']; w.format = 'json'
                w.save()
            except: pass

    return redirect(url_for('index', shop=shop_url))

@app.route('/api/save_settings', methods=['POST'])
def save_settings():
    data = request.json
    shop = Shop.query.filter_by(shop_url=data.get('shop_url')).first()
    if not shop: return jsonify({'error': 'Shop not found'}), 404
    
    if 'odoo_url' in data:
        shop.odoo_url = data['odoo_url']
        shop.odoo_db = data['odoo_db']
        shop.odoo_username = data['odoo_username']
        shop.odoo_password = data['odoo_password']
        shop.odoo_company_id = data.get('odoo_company_id')
        db.session.commit()
    
    for key in ['inventory_field', 'sync_zero_stock', 'inventory_locations', 'shopify_location_id', 
                'prod_sync_title', 'prod_sync_desc', 'prod_sync_price']:
        if key in data: set_shop_config(shop.id, key, data[key])

    try:
        OdooClient(shop.odoo_url, shop.odoo_db, shop.odoo_username, shop.odoo_password)
        return jsonify({'message': 'Connected Successfully'})
    except Exception as e:
        return jsonify({'error': f'Connection Failed: {str(e)}'}), 400

@app.route('/api/logs/live', methods=['GET'])
def api_live_logs():
    shop = Shop.query.filter_by(shop_url=request.args.get('shop_url')).first()
    if not shop: return jsonify([])
    logs = SyncLog.query.filter_by(shop_id=shop.id).order_by(SyncLog.timestamp.desc()).limit(50).all()
    return jsonify([{'id': l.id, 'timestamp': l.timestamp.isoformat(), 'message': f"[{l.entity}] {l.message}", 'type': 'info'} for l in logs])

@app.route('/webhook/orders/updated', methods=['POST'])
def webhook_orders():
    if not verify_webhook(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    shop = Shop.query.filter_by(shop_url=request.headers.get('X-Shopify-Shop-Domain')).first()
    if not shop: return "Shop not found", 200
    
    odoo = get_odoo_connection(shop)
    if odoo: process_order_data(request.json, shop, odoo)
    return "OK", 200

@app.route('/webhook/products/update', methods=['POST'])
def webhook_products():
    if not verify_webhook(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    shop = Shop.query.filter_by(shop_url=request.headers.get('X-Shopify-Shop-Domain')).first()
    if shop:
        odoo = get_odoo_connection(shop)
        if odoo: process_product_data(request.json, shop, odoo)
    return "OK", 200

@app.route('/gdpr/customers/data_request', methods=['POST'])
def gdpr_data(): return jsonify({"message": "Received"}), 200
@app.route('/gdpr/customers/redact', methods=['POST'])
def gdpr_cust_redact(): return jsonify({"message": "Received"}), 200
@app.route('/gdpr/shop/redact', methods=['POST'])
def gdpr_shop_redact(): return jsonify({"message": "Received"}), 200

# --- SETUP ROUTE ---
@app.route('/init_db')
def init_db():
    with app.app_context(): db.create_all()
    return "Tables Created", 200

if __name__ == '__main__':
    with app.app_context(): db.create_all()
    app.run(debug=True)
