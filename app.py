import os
import hmac
import hashlib
import base64
import json
import threading
import schedule
import time
import shopify
import shopify.api_access
import shopify.session
from flask import Flask, request, jsonify, render_template, redirect, url_for, session
from models import db, ProductMap, SyncLog, AppSetting, CustomerMap, Shop
from odoo_client import OdooClient
import requests
from datetime import datetime, timedelta

# --- CRITICAL FIX: FORCE SHOPIFY TO ACCEPT NEW SCOPES ---
# This overrides the library's strict validation to prevent crashes.
class PermissiveApiAccess(shopify.api_access.ApiAccess):
    def __init__(self, scopes):
        if isinstance(scopes, str):
            self._scopes = set(scope.strip() for scope in scopes.split(","))
        else:
            self._scopes = set(scopes)
    def __iter__(self): return iter(self._scopes)
    def __str__(self): return ", ".join(self._scopes)

# Apply patch to BOTH modules
shopify.api_access.ApiAccess = PermissiveApiAccess
shopify.session.ApiAccess = PermissiveApiAccess
# --------------------------------------------------------

app = Flask(__name__)
app.secret_key = os.urandom(24)

# --- CONFIG ---
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_SECRET = os.getenv('SHOPIFY_SECRET')
APP_URL = os.getenv('APP_URL')

# Database Config
database_url = os.getenv('DATABASE_URL', 'sqlite:///local.db')
if database_url and database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql+pg8000://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
shopify.Session.setup(api_key=SHOPIFY_API_KEY, secret=SHOPIFY_SECRET)

# Global Locks
order_processing_lock = threading.Lock()
active_processing_ids = set()

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

# --- GRAPHQL HELPERS ---
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
        
        # Check Existing
        existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
        existing_order_id = existing_ids[0] if existing_ids else None

        # Partner Logic
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

        # Lines
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

        # Shipping
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

def task_sync_inventory(shop, odoo):
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

# --- SCHEDULER ---
def run_scheduler_jobs():
    with app.app_context():
        shops = Shop.query.filter(Shop.access_token != None).all()
        for shop in shops:
            odoo = get_odoo_connection(shop)
            if not odoo: continue
            threading.Thread(target=task_sync_inventory, args=(shop, odoo)).start()

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

# --- API ENDPOINTS (DASHBOARD) ---

@app.route('/api/get_settings', methods=['GET'])
def get_settings():
    shop_url = request.args.get('shop_url')
    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop: return jsonify({})

    keys = [
        'odoo_company_id', 'inventory_field', 'sync_zero_stock', 'inventory_locations', 
        'prod_sync_price', 'prod_sync_title', 'prod_sync_desc', 
        'prod_sync_images', 'prod_auto_create'
    ]
    data = {
        'odoo_url': shop.odoo_url,
        'odoo_db': shop.odoo_db,
        'odoo_username': shop.odoo_username,
        'has_password': bool(shop.odoo_password) 
    }
    
    for k in keys:
        val = get_shop_config(shop.id, k)
        if val is not None: data[k] = val
        
    return jsonify(data)

@app.route('/api/save_settings', methods=['POST'])
def save_settings():
    data = request.json
    shop_url = data.get('shop_url')
    if not shop_url: return jsonify({'error': 'Missing shop_url'}), 400

    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop: return jsonify({'error': 'Shop not found'}), 404
    
    if 'odoo_url' in data:
        shop.odoo_url = data['odoo_url']
        shop.odoo_db = data['odoo_db']
        shop.odoo_username = data['odoo_username']
        if data.get('odoo_password'): 
            shop.odoo_password = data['odoo_password']
        shop.odoo_company_id = data.get('odoo_company_id')
        db.session.commit()
    
    keys = ['inventory_field', 'sync_zero_stock', 'inventory_locations', 'prod_sync_price', 
            'prod_sync_title', 'prod_sync_desc', 'prod_sync_images', 'prod_auto_create']
    for key in keys:
        if key in data: set_shop_config(shop.id, key, data[key])

    # Test & Log Connection
    if 'odoo_username' in data:
        try:
            OdooClient(shop.odoo_url, shop.odoo_db, shop.odoo_username, shop.odoo_password)
            log_event(shop.id, 'Connection', 'Success', 'Odoo Connection Verified') 
            return jsonify({'message': 'Settings Saved & Connection Verified'})
        except Exception as e:
            log_event(shop.id, 'Connection', 'Error', f'Connection Failed: {str(e)}')
            return jsonify({'error': f'Settings Saved but Connection Failed: {str(e)}'}), 400
            
    return jsonify({'message': 'Settings Saved'})

@app.route('/api/orders/recent', methods=['GET'])
def get_recent_orders():
    shop_url = request.args.get('shop_url')
    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop: return jsonify([])

    orders_data = []
    try:
        with shopify.Session.temp(shop.shop_url, '2024-01', shop.access_token):
            orders = shopify.Order.find(limit=20, status='any', order="created_at DESC")
            
            for o in orders:
                log = SyncLog.query.filter(SyncLog.shop_id == shop.id, SyncLog.message.contains(o.name)).first()
                status = 'Synced' if log and 'Success' in log.status else 'Pending'
                if log and 'Error' in log.status: status = 'Error'
                
                orders_data.append({
                    'id': o.id,
                    'name': o.name,
                    'created_at': o.created_at,
                    'total': o.total_price,
                    'financial_status': o.financial_status,
                    'sync_status': status
                })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify(orders_data)

@app.route('/api/orders/sync', methods=['POST'])
def manual_sync_order():
    shop_url = request.json.get('shop_url')
    order_id = request.json.get('order_id')
    
    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop: return jsonify({'error': 'Shop not found'}), 404

    try:
        odoo = get_odoo_connection(shop)
        if not odoo: return jsonify({'error': 'Cannot connect to Odoo'}), 400

        with shopify.Session.temp(shop.shop_url, '2024-01', shop.access_token):
            order = shopify.Order.find(order_id)
            if not order: return jsonify({'error': 'Order not found in Shopify'}), 404
            
            success = process_order_data(order.to_dict(), shop, odoo)
            
            if success:
                return jsonify({'message': f'Order {order.name} synced successfully'})
            else:
                return jsonify({'error': 'Sync failed (Check logs)'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/logs/live', methods=['GET'])
def api_live_logs():
    shop_url = request.args.get('shop_url')
    if not shop_url: return jsonify([])
    
    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop: return jsonify([])
    
    logs = SyncLog.query.filter_by(shop_id=shop.id).order_by(SyncLog.id.desc()).limit(50).all()
    return jsonify([{'id': l.id, 'timestamp': l.timestamp.isoformat(), 'message': f"[{l.entity}] {l.message}", 'status': l.status} for l in logs])

# --- WEBHOOKS ---

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

# --- GDPR & SYSTEM ---

@app.route('/gdpr/customers/data_request', methods=['POST'])
def gdpr_data(): return jsonify({"message": "Received"}), 200
@app.route('/gdpr/customers/redact', methods=['POST'])
def gdpr_cust_redact(): return jsonify({"message": "Received"}), 200
@app.route('/gdpr/shop/redact', methods=['POST'])
def gdpr_shop_redact(): return jsonify({"message": "Received"}), 200

@app.route('/init_db')
def init_db():
    with app.app_context(): db.create_all()
    return "Tables Created", 200

if __name__ == '__main__':
    with app.app_context(): db.create_all()
    app.run(debug=True)
