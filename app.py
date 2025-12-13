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
APP_URL = os.getenv('APP_URL') # e.g. https://tripster-odoo-connector.onrender.com

# --- DATABASE CONFIG ---
database_url = os.getenv('DATABASE_URL', 'sqlite:///local.db')
if database_url and database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql+pg8000://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)

# Initialize Shopify Library
shopify.Session.setup(api_key=SHOPIFY_API_KEY, secret=SHOPIFY_SECRET)

# --- GLOBAL LOCKS ---
order_processing_lock = threading.Lock()
active_processing_ids = set()

# --- HELPERS ---

def get_shop_config(shop_id, key, default=None):
    """Retrieve setting for a specific shop"""
    try:
        setting = AppSetting.query.filter_by(shop_id=shop_id, key=key).first()
        if not setting: return default
        try: return json.loads(setting.value)
        except: return setting.value
    except: return default

def set_shop_config(shop_id, key, value):
    """Save setting for a specific shop"""
    try:
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
    """Creates an OdooClient instance for a specific shop"""
    if not shop.odoo_url or not shop.odoo_password:
        return None
    try:
        return OdooClient(
            url=shop.odoo_url,
            db=shop.odoo_db,
            username=shop.odoo_username,
            password=shop.odoo_password
        )
    except Exception as e:
        print(f"Odoo Connect Error for {shop.shop_url}: {e}")
        return None

def verify_webhook(data, hmac_header):
    if not SHOPIFY_SECRET: return True # Dev mode
    digest = hmac.new(SHOPIFY_SECRET.encode('utf-8'), data, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), hmac_header)

def log_event(shop_id, entity, status, message):
    try:
        log = SyncLog(shop_id=shop_id, entity=entity, status=status, message=message, timestamp=datetime.utcnow())
        db.session.add(log)
        db.session.commit()
    except: 
        db.session.rollback()

def extract_id(res):
    if isinstance(res, list) and len(res) > 0: return res[0]
    return res

# --- GRAPHQL HELPERS (Context Aware) ---
def find_shopify_product_by_sku(sku):
    # Relies on active session being set by the caller
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
    # Relies on active session being set by the caller
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

# --- CORE B2B LOGIC (Refactored for Multi-Tenant) ---

def process_order_data(data, shop, odoo):
    """Syncs order using Shop-specific credentials."""
    shopify_id = str(data.get('id', ''))
    shopify_name = data.get('name')
    
    if shopify_id in active_processing_ids: return False, "Skipped"
    active_processing_ids.add(shopify_id)

    try:
        email = data.get('email') or data.get('contact_email')
        client_ref = f"ONLINE_{shopify_name}"
        company_id = shop.odoo_company_id 
        
        # Check Existing
        existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
        existing_order_id = existing_ids[0] if existing_ids else None

        # --- PARTNER LOGIC (B2B Forced) ---
        partner = odoo.search_partner_by_email(email)
        
        if not partner:
            cust_data = data.get('customer', {})
            def_address = data.get('billing_address') or data.get('shipping_address') or {}
            
            company_name = def_address.get('company')
            person_name = f"{cust_data.get('first_name', '')} {cust_data.get('last_name', '')}".strip()
            final_name = company_name if company_name else (person_name or email)

            vat_number = None
            for attr in data.get('note_attributes', []):
                if attr.get('name', '').lower() in ['vat', 'vat_number', 'tax_id']:
                    vat_number = attr.get('value')

            vals = {
                'name': final_name, 'email': email, 'phone': cust_data.get('phone'),
                'street': def_address.get('address1'), 'city': def_address.get('city'),
                'zip': def_address.get('zip'), 'country_code': def_address.get('country_code'),
                'vat': vat_number,
                'is_company': True, 'company_type': 'company' # Force B2B
            }
            if company_id: vals['company_id'] = int(company_id)
            
            try:
                partner_id = odoo.create_partner(vals)
                partner = {'id': partner_id, 'name': final_name}
                log_event(shop.id, 'Customer', 'Success', f"Created Partner: {final_name}")
                
                if shopify_id:
                    c_id = str(data.get('customer', {}).get('id'))
                    if c_id:
                        db.session.add(CustomerMap(shopify_customer_id=c_id, shop_id=shop.id, odoo_partner_id=partner_id, email=email))
                        db.session.commit()
            except Exception as e: return False, f"Customer Error: {e}"
        
        partner_id = extract_id(partner['parent_id'][0] if partner.get('parent_id') else partner['id'])

        # Child Addresses
        bill_addr = data.get('billing_address') or {}
        ship_addr = data.get('shipping_address') or {}
        
        def prep_child(addr, label):
            dname = addr.get('name') or partner['name']
            if dname == partner['name']: dname = f"{dname} ({label})"
            return {
                'name': dname, 'street': addr.get('address1'), 'city': addr.get('city'),
                'zip': addr.get('zip'), 'country_code': addr.get('country_code'),
                'phone': addr.get('phone'), 'email': email
            }

        invoice_id = odoo.find_or_create_child_address(partner_id, prep_child(bill_addr, "Invoice"), 'invoice') if bill_addr else partner_id
        shipping_id = odoo.find_or_create_child_address(partner_id, prep_child(ship_addr, "Delivery"), 'delivery') if ship_addr else partner_id
        sales_rep_id = odoo.get_partner_salesperson(partner_id) or odoo.uid

        # --- LINE ITEMS ---
        lines = []
        for item in data.get('line_items', []):
            sku = item.get('sku')
            if not sku: continue
            product_id = odoo.search_product_by_sku(sku, company_id)
            if not product_id:
                try:
                    new_vals = {'name': item['name'], 'default_code': sku, 'list_price': float(item.get('price', 0)), 'type': 'product'}
                    if company_id: new_vals['company_id'] = int(company_id)
                    odoo.create_product(new_vals)
                    product_id = odoo.search_product_by_sku(sku, company_id)
                except: pass
            
            if product_id:
                price = float(item.get('price', 0))
                qty = int(item.get('quantity', 1))
                disc = float(item.get('total_discount', 0))
                pct = (disc / (price * qty)) * 100 if price > 0 else 0.0
                lines.append((0, 0, {'product_id': product_id, 'product_uom_qty': qty, 'price_unit': price, 'name': item['name'], 'discount': pct}))

        # Shipping Line
        for ship in data.get('shipping_lines', []):
            cost = float(ship.get('price', 0.0))
            if cost >= 0:
                ship_pid = odoo.search_product_by_sku("SHIP_FEE", company_id) or odoo.search_product_by_name("Shopify Shipping", company_id)
                if not ship_pid:
                    try:
                        odoo.create_service_product("Shopify Shipping", company_id)
                        ship_pid = odoo.search_product_by_name("Shopify Shipping", company_id)
                    except: pass
                if ship_pid:
                    lines.append((0, 0, {'product_id': ship_pid, 'product_uom_qty': 1, 'price_unit': cost, 'name': ship.get('title', 'Shipping'), 'discount': 0.0}))

        if not lines: return False, "No Lines"

        gateway = data.get('gateway') or (data.get('payment_gateway_names') or ['Shopify'])[0]
        note = f"Payment: {gateway}"

        if existing_order_id:
            order_info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'read', [[existing_order_id]], {'fields': ['state']})
            if order_info and order_info[0]['state'] not in ['done', 'cancel']:
                vals = {'order_line': [(5,0,0)] + lines, 'partner_shipping_id': shipping_id, 'partner_invoice_id': invoice_id, 'note': note}
                odoo.update_sale_order(existing_order_id, vals)
                log_event(shop.id, 'Order', 'Success', f"Updated {client_ref}")
        else:
            vals = {
                'name': client_ref, 'client_order_ref': client_ref, 'partner_id': partner_id,
                'partner_invoice_id': invoice_id, 'partner_shipping_id': shipping_id,
                'order_line': lines, 'user_id': sales_rep_id, 'state': 'draft', 'note': note
            }
            if company_id: vals['company_id'] = int(company_id)
            odoo.create_sale_order(vals, context={'manual_price': True})
            log_event(shop.id, 'Order', 'Success', f"Created {client_ref}")

        return True, "Done"

    except Exception as e:
        log_event(shop.id, 'Order', 'Error', str(e))
        return False, str(e)
    finally:
        active_processing_ids.discard(shopify_id)

def process_product_sync(shop, odoo):
    """Sync Products logic for a specific shop"""
    company_id = shop.odoo_company_id
    odoo_products = odoo.get_all_products(company_id)
    active_odoo_skus = set()
    
    # Load settings
    sync_title = get_shop_config(shop.id, 'prod_sync_title', True)
    sync_desc = get_shop_config(shop.id, 'prod_sync_desc', True)
    sync_price = get_shop_config(shop.id, 'prod_sync_price', True)
    
    synced = 0
    with shopify.Session.temp(shop.shop_url, '2024-01', shop.access_token):
        for p in odoo_products:
            sku = p.get('default_code')
            if not sku: continue
            active_odoo_skus.add(sku)
            
            shopify_id = find_shopify_product_by_sku(sku)
            try:
                if shopify_id: sp = shopify.Product.find(shopify_id)
                else: sp = shopify.Product()
                changed = False
                
                if sync_title and sp.title != p['name']: sp.title = p['name']; changed=True
                if sync_desc and (sp.body_html or '') != (p.get('description_sale') or ''): sp.body_html = p.get('description_sale'); changed=True
                
                if changed or not shopify_id: sp.save()
                
                # Variant sync
                if not shopify_id: sp = shopify.Product.find(sp.id)
                variant = sp.variants[0] if sp.variants else shopify.Variant(prefix_options={'product_id': sp.id})
                
                v_changed = False
                if variant.sku != sku: variant.sku = sku; v_changed=True
                if sync_price and variant.price != str(p['list_price']): variant.price = str(p['list_price']); v_changed=True
                
                if v_changed: variant.save()
                synced += 1
            except Exception as e:
                log_event(shop.id, 'Product', 'Error', f"Failed {sku}: {e}")
                
    log_event(shop.id, 'Product Sync', 'Success', f"Synced {synced} products")

def process_customer_sync(shop, odoo):
    """Sync Customers logic for a specific shop"""
    last_run = "2000-01-01 00:00:00" # In real app, store last run time in DB
    odoo_customers = odoo.get_changed_customers(last_run, shop.odoo_company_id)
    synced = 0
    
    with shopify.Session.temp(shop.shop_url, '2024-01', shop.access_token):
        for p in odoo_customers:
            email = p.get('email')
            if not email or "@" not in email: continue
            
            try:
                sc = shopify.Customer.search(query=f"email:{email}")
                c = sc[0] if sc else shopify.Customer()
                c.email = email
                c.first_name = p.get('name', '').split(' ')[0]
                c.last_name = ' '.join(p.get('name', '').split(' ')[1:]) or 'Customer'
                
                # Metafields
                metas = []
                if p.get('vat'): metas.append(shopify.Metafield({'key': 'vat', 'value': p['vat'], 'type': 'single_line_text_field', 'namespace': 'custom'}))
                if p.get('user_id'): metas.append(shopify.Metafield({'key': 'salesrep', 'value': p['user_id'][1], 'type': 'single_line_text_field', 'namespace': 'custom'}))
                
                if metas: c.metafields = metas
                c.save()
                synced += 1
            except Exception as e:
                log_event(shop.id, 'Customer', 'Error', f"Failed {email}: {e}")
                
    log_event(shop.id, 'Customer Sync', 'Success', f"Synced {synced} customers")

# --- AUTHENTICATION ROUTES (OAuth) ---

@app.route('/')
def index():
    shop_url = request.args.get('shop')
    if shop_url:
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if shop and shop.access_token:
            return render_template('dashboard.html', shop=shop) 
        return redirect(url_for('auth', shop=shop_url))
    return "Tripster Odoo Connector: Please install via Shopify."

@app.route('/shopify/auth')
def auth():
    shop_url = request.args.get('shop')
    if not shop_url: return "Missing shop", 400
    
    scopes = ['read_products', 'write_products', 'read_orders', 'write_orders', 'read_customers', 'write_customers', 'read_inventory', 'write_inventory']
    session = shopify.Session(shop_url, '2024-01')
    perm_url = session.create_permission_url(scopes, url_for('callback', _external=True))
    return redirect(perm_url)

@app.route('/shopify/callback')
def callback():
    shop_url = request.args.get('shop')
    try:
        session = shopify.Session(shop_url, '2024-01')
        token = session.request_token(request.args)
        
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop:
            shop = Shop(shop_url=shop_url)
            db.session.add(shop)
        
        shop.access_token = token
        shop.is_active = True
        db.session.commit()
        
        # Register Webhooks
        with shopify.Session.temp(shop_url, '2024-01', token):
            hooks = [
                {'topic': 'orders/updated', 'address': f'{APP_URL}/webhook/orders/updated'},
                {'topic': 'products/update', 'address': f'{APP_URL}/webhook/products/update'},
                {'topic': 'app/uninstalled', 'address': f'{APP_URL}/webhook/app/uninstalled'}
            ]
            for h in hooks:
                webhook = shopify.Webhook()
                webhook.topic = h['topic']
                webhook.address = h['address']
                webhook.format = 'json'
                try: webhook.save()
                except: pass

        return redirect(url_for('index', shop=shop_url))
    except Exception as e:
        return f"Auth Error: {e}", 500

# --- API ROUTES (Settings) ---

@app.route('/api/save_settings', methods=['POST'])
def save_settings():
    data = request.json
    shop_url = data.get('shop_url')
    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop: return jsonify({'error': 'Shop not found'}), 404
    
    if 'odoo_url' in data:
        shop.odoo_url = data['odoo_url']
        shop.odoo_db = data['odoo_db']
        shop.odoo_username = data['odoo_username']
        shop.odoo_password = data['odoo_password']
        shop.odoo_company_id = data.get('odoo_company_id')
        db.session.commit()
    
    # Save other configs via set_shop_config (omitted for brevity, follow pattern)
    
    try:
        OdooClient(shop.odoo_url, shop.odoo_db, shop.odoo_username, shop.odoo_password)
        return jsonify({'message': 'Connected Successfully'})
    except Exception as e:
        return jsonify({'error': f'Connection Failed: {str(e)}'}), 400

@app.route('/api/logs/live', methods=['GET'])
def api_live_logs():
    shop_url = request.args.get('shop_url')
    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop: return jsonify([])
    
    logs = SyncLog.query.filter_by(shop_id=shop.id).order_by(SyncLog.timestamp.desc()).limit(100).all()
    data = []
    for log in logs:
        data.append({'id': log.id, 'timestamp': log.timestamp.isoformat(), 'message': f"[{log.entity}] {log.message}", 'type': 'info', 'details': log.status})
    return jsonify(data)

# --- WEBHOOKS ---

@app.route('/webhook/orders/updated', methods=['POST'])
def webhook_orders():
    if not verify_webhook(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    
    shop_domain = request.headers.get('X-Shopify-Shop-Domain')
    shop = Shop.query.filter_by(shop_url=shop_domain).first()
    if not shop or not shop.is_active: return "Shop not found", 200
    
    odoo = get_odoo_connection(shop)
    if not odoo: return "Odoo not configured", 200
    
    process_order_data(request.json, shop, odoo)
    return "OK", 200

# --- GDPR (Mandatory) ---
@app.route('/gdpr/customers/data_request', methods=['POST'])
def gdpr_data(): return jsonify({"message": "Received"}), 200
@app.route('/gdpr/customers/redact', methods=['POST'])
def gdpr_cust_redact(): return jsonify({"message": "Received"}), 200
@app.route('/gdpr/shop/redact', methods=['POST'])
def gdpr_shop_redact(): 
    # Clean up shop data from DB
    return jsonify({"message": "Received"}), 200

# --- SCHEDULER TASKS ---

def task_sync_all():
    """Loops through ALL active shops and syncs data"""
    with app.app_context():
        shops = Shop.query.filter_by(is_active=True).all()
        for shop in shops:
            odoo = get_odoo_connection(shop)
            if not odoo: continue
            
            # Inventory Logic
            # (Insert perform_inventory_sync logic here using odoo.get_product_ids_with_recent_stock_moves)
            pass 

def run_schedule():
    schedule.every(30).minutes.do(lambda: threading.Thread(target=task_sync_all).start())
    while True:
        schedule.run_pending()
        time.sleep(1)

# --- START ---
t = threading.Thread(target=run_schedule, daemon=True)
t.start()

if __name__ == '__main__':
    with app.app_context(): db.create_all()
    app.run(debug=True)
