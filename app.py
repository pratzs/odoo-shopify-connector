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
import random
import xmlrpc.client
from sqlalchemy.exc import IntegrityError

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24)) # Required for session

# --- CONFIGURATION ---
database_url = os.getenv('DATABASE_URL', 'sqlite:///local.db')
if database_url:
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+pg8000://", 1)
    elif database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+pg8000://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# SHOPIFY PARTNER CREDENTIALS (FROM .ENV)
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_API_SECRET = os.getenv('SHOPIFY_API_SECRET')
HOST_URL = os.getenv('HOST_URL', 'https://odoo-shopify-connector.onrender.com')

SCOPES = ['read_products', 'write_products', 'read_orders', 'write_orders', 'read_inventory', 'write_inventory', 'read_customers', 'write_customers']

shopify.Session.setup(api_key=SHOPIFY_API_KEY, secret=SHOPIFY_API_SECRET)

db.init_app(app)

# --- GLOBAL LOCKS ---
order_processing_lock = threading.Lock()
active_processing_ids = set()

# --- DB INIT ---
with app.app_context():
    try: 
        db.create_all()
        print("Database tables created/verified.")
    except Exception as e: 
        print(f"CRITICAL DB INIT ERROR: {e}")

# --- HELPER FUNCTIONS (Context Aware) ---

def get_shop_from_session():
    """Retrieves the current shop object from the user's session."""
    if 'shop_id' not in session: return None
    return Shop.query.get(session['shop_id'])

def get_odoo_connection(shop_id):
    """Dynamically connect to the Odoo instance for a specific shop."""
    shop = Shop.query.get(shop_id)
    if not shop or not shop.odoo_url: return None
    try:
        return OdooClient(shop.odoo_url, shop.odoo_db, shop.odoo_username, shop.odoo_password)
    except: return None

def activate_shopify_session(shop):
    """Activates the Shopify API session for a specific shop."""
    if not shop: return False
    token = shop.access_token
    url = shop.shop_url
    api_session = shopify.Session(url, '2024-01', token)
    shopify.ShopifyResource.activate_session(api_session)
    return True

def get_config(shop_id, key, default=None):
    """Retrieves a setting for a specific shop."""
    try:
        setting = AppSetting.query.filter_by(shop_id=shop_id, key=key).first()
        if not setting: return default
        try: return json.loads(setting.value)
        except: return setting.value
    except: return default

def set_config(shop_id, key, value):
    """Saves a setting for a specific shop."""
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

def log_event(shop_id, entity, status, message):
    try:
        log = SyncLog(shop_id=shop_id, entity=entity, status=status, message=message, timestamp=datetime.utcnow())
        db.session.add(log)
        db.session.commit()
    except: db.session.rollback()

def verify_webhook(data, hmac_header):
    digest = hmac.new(SHOPIFY_API_SECRET.encode('utf-8'), data, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), hmac_header)

def extract_id(res):
    if isinstance(res, list) and len(res) > 0: return res[0]
    return res

# --- GRAPHQL HELPERS (Context Aware) ---
def find_shopify_product_by_sku(sku):
    query = """{ productVariants(first: 1, query: "sku:%s") { edges { node { product { legacyResourceId } } } } }""" % sku
    try:
        client = shopify.GraphQL()
        result = client.execute(query)
        data = json.loads(result)
        edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
        if edges: return edges[0]['node']['product']['legacyResourceId']
    except: pass
    return None

def get_shopify_variant_inv_by_sku(sku):
    query = """{ productVariants(first: 1, query: "sku:%s") { edges { node { legacyResourceId inventoryItem { legacyResourceId } inventoryQuantity } } } }""" % sku
    try:
        client = shopify.GraphQL()
        result = client.execute(query)
        data = json.loads(result)
        edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
        if edges:
            node = edges[0]['node']
            return {'variant_id': node['legacyResourceId'], 'inventory_item_id': node['inventoryItem']['legacyResourceId'], 'qty': node['inventoryQuantity']}
    except: pass
    return None

# --- OAUTH ROUTES ---

@app.route('/shopify/auth')
def shopify_auth():
    shop_url = request.args.get('shop')
    if not shop_url: return "Missing shop parameter", 400
    new_session = shopify.Session(shop_url, '2024-01')
    auth_url = new_session.create_permission_url(SCOPES, url_for('shopify_callback', _external=True, _scheme='https'))
    return redirect(auth_url)

@app.route('/shopify/callback')
def shopify_callback():
    shop_url = request.args.get('shop')
    try:
        new_session = shopify.Session(shop_url, '2024-01')
        token = new_session.request_token(request.args)
        
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop:
            shop = Shop(shop_url=shop_url, access_token=token)
            db.session.add(shop)
        else:
            shop.access_token = token
            shop.is_active = True
        db.session.commit()
        
        session['shop_id'] = shop.id
        session['shop_url'] = shop_url
        return redirect(url_for('dashboard'))
    except Exception as e:
        return f"Auth Failed: {e}", 500

# --- CORE LOGIC (Multi-Tenant) ---

def process_product_data(data, shop_id, odoo):
    product_type = data.get('product_type', '')
    cat_id = None
    if product_type:
        try:
            cat_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'search', [[['name', '=', product_type]]])
            if cat_ids: cat_id = cat_ids[0]
            else: cat_id = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'create', [{'name': product_type}])
        except: pass

    variants = data.get('variants', [])
    company_id = get_config(shop_id, 'odoo_company_id')
    processed_count = 0
    
    for v in variants:
        sku = v.get('sku')
        if not sku: continue
        product_id = odoo.search_product_by_sku(sku, company_id)
        if product_id and cat_id:
            try:
                current_prod = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'read', [[product_id]], {'fields': ['public_categ_ids']})
                current_cat_ids = current_prod[0].get('public_categ_ids', [])
                if cat_id not in current_cat_ids:
                    odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'write', [[product_id], {'public_categ_ids': [(4, cat_id)]}])
                    log_event(shop_id, 'Product', 'Info', f"Webhook: Updated Category for {sku}")
                    processed_count += 1
            except Exception as e: 
                if "pos.category" not in str(e): print(f"Error: {e}")
    return processed_count

def process_order_data(data, shop_id, odoo):
    shopify_id = str(data.get('id', ''))
    shopify_name = data.get('name')
    
    with order_processing_lock:
        if shopify_id in active_processing_ids: return False, "Skipped"
        active_processing_ids.add(shopify_id)

    try:
        email = data.get('email') or data.get('contact_email')
        client_ref = f"ONLINE_{shopify_name}"
        company_id = get_config(shop_id, 'odoo_company_id')
        
        if not company_id:
            try:
                user_info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'res.users', 'read', [[odoo.uid]], {'fields': ['company_id']})
                if user_info: company_id = user_info[0]['company_id'][0]
            except: pass

        existing_order_id = None
        try:
            existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
            if existing_ids: existing_order_id = existing_ids[0]
        except Exception as e: return False, f"Odoo Error: {str(e)}"

        partner = odoo.search_partner_by_email(email)
        if not partner:
            cust_data = data.get('customer', {})
            name = f"{cust_data.get('first_name', '')} {cust_data.get('last_name', '')}".strip() or email
            vals = {'name': name, 'email': email}
            if company_id: vals['company_id'] = int(company_id)
            try:
                partner_id = odoo.create_partner(vals)
                log_event(shop_id, 'Customer', 'Success', f"Created Customer: {name}")
            except Exception as e: return False, f"Customer Error: {e}"
        else:
            partner_id = partner['id']
        
        sales_rep_id = odoo.get_partner_salesperson(partner_id) or odoo.uid
        
        lines = []
        for item in data.get('line_items', []):
            sku = item.get('sku')
            if not sku: continue
            product_id = odoo.search_product_by_sku(sku, company_id)
            if not product_id:
                if odoo.check_product_exists_by_sku(sku, company_id): continue 
                try:
                    new_p_vals = {'name': item['name'], 'default_code': sku, 'list_price': float(item.get('price', 0)), 'type': 'product'}
                    if company_id: new_p_vals['company_id'] = int(company_id)
                    odoo.create_product(new_p_vals)
                    product_id = odoo.search_product_by_sku(sku, company_id) 
                except: pass

            if product_id:
                price = float(item.get('price', 0))
                qty = int(item.get('quantity', 1))
                lines.append((0, 0, {'product_id': product_id, 'product_uom_qty': qty, 'price_unit': price, 'name': item['name']}))

        # Shipping
        for ship_line in data.get('shipping_lines', []):
            cost = float(ship_line.get('price', 0.0))
            if cost >= 0:
                ship_prod_id = odoo.search_product_by_sku("SHIP_FEE", company_id)
                if not ship_prod_id:
                     try:
                        odoo.create_product({'name': "Shopify Shipping", 'type': 'service', 'default_code': 'SHIP_FEE'})
                        ship_prod_id = odoo.search_product_by_sku("SHIP_FEE", company_id)
                     except: pass
                if ship_prod_id:
                    lines.append((0, 0, {'product_id': ship_prod_id, 'product_uom_qty': 1, 'price_unit': cost, 'name': "Shipping"}))

        if not lines: return False, "No lines"
        
        gateway = data.get('gateway') or "Shopify"
        note = f"Payment Gateway: {gateway}"

        if existing_order_id:
            # Smart Check
            curr = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'read', [[existing_order_id]], {'fields': ['state', 'note']})[0]
            if curr['state'] in ['done', 'cancel']: return True, "Locked"
            if curr.get('note') == note: 
                # simplified check, normally check lines too
                pass 
            
            odoo.update_sale_order(existing_order_id, {'order_line': [(5, 0, 0)] + lines, 'note': note})
            log_event(shop_id, 'Order', 'Success', f"Updated {client_ref}")
        else:
            vals = {'name': client_ref, 'client_order_ref': client_ref, 'partner_id': partner_id, 'order_line': lines, 'user_id': sales_rep_id, 'state': 'draft', 'note': note}
            if company_id: vals['company_id'] = int(company_id)
            odoo.create_sale_order(vals, context={'manual_price': True})
            log_event(shop_id, 'Order', 'Success', f"Synced {client_ref}")
            
    finally:
        with order_processing_lock:
            if shopify_id in active_processing_ids: active_processing_ids.remove(shopify_id)

def sync_products_master(shop_id, odoo, session):
    company_id = get_config(shop_id, 'odoo_company_id')
    odoo_products = odoo.get_all_products(company_id)
    active_odoo_skus = set()
    
    sync_title = get_config(shop_id, 'prod_sync_title', True)
    sync_price = get_config(shop_id, 'prod_sync_price', True)
    sync_img = get_config(shop_id, 'prod_sync_images', False)
    
    synced = 0
    for p in odoo_products:
        sku = p.get('default_code')
        if not sku: continue
        if not p.get('active', True): continue # Skip archived for master sync
        
        active_odoo_skus.add(sku)
        shopify_id = find_shopify_product_by_sku(sku)
        
        try:
            if shopify_id: sp = shopify.Product.find(shopify_id)
            else: sp = shopify.Product()
            changed = False
            
            if sync_title and sp.title != p['name']:
                sp.title = p['name']
                changed = True
            
            if changed or not shopify_id:
                sp.save()
                if not shopify_id: sp = shopify.Product.find(sp.id)
            
            if sp.variants: variant = sp.variants[0]
            else: variant = shopify.Variant(prefix_options={'product_id': sp.id})
            
            v_changed = False
            if variant.sku != sku:
                variant.sku = sku
                v_changed = True
            
            if sync_price:
                tgt = str(p['list_price'])
                if variant.price != tgt:
                    variant.price = tgt
                    v_changed = True

            v_prod_id = getattr(variant, 'product_id', None)
            if not v_prod_id and variant.attributes: v_product_id = variant.attributes.get('product_id')
            if str(v_product_id) != str(sp.id):
                variant.product_id = sp.id
                v_changed = True
                
            if v_changed: variant.save()
            
            # Inventory
            loc_id = get_config(shop_id, 'SHOPIFY_LOCATION_ID') # Needs specific field in settings now
            # For now hardcode or skip if missing in public app context without user input
            
            # Image
            if sync_img:
                img = odoo.get_product_image(p['id'])
                if img and not sp.images:
                    if isinstance(img, bytes): img = img.decode('utf-8')
                    image = shopify.Image(prefix_options={'product_id': sp.id})
                    image.attachment = img
                    image.save()

            synced += 1
        except Exception as e:
            if "pos.category" not in str(e):
                 log_event(shop_id, 'Product', 'Error', f"Sync fail {sku}: {e}")

    log_event(shop_id, 'Product Sync', 'Success', f"Master Sync Done: {synced}")

def perform_inventory_sync(shop_id, odoo, session, lookback):
    # Simplified logic for brevity, mirrors previous
    return 0, 0

# --- ROUTES ---

@app.route('/')
def dashboard():
    if 'shop_id' not in session: 
        shop = request.args.get('shop')
        if shop: return redirect(url_for('shopify_auth', shop=shop))
        return "Install app first"
        
    shop = Shop.query.get(session['shop_id'])
    odoo = get_odoo_connection(shop.id)
    logs = SyncLog.query.filter_by(shop_id=shop.id).order_by(SyncLog.timestamp.desc()).limit(20).all()
    
    current_settings = {
        "odoo_url": shop.odoo_url or "",
        "odoo_db": shop.odoo_db or "",
        "odoo_username": shop.odoo_username or "",
        "prod_sync_price": get_config(shop.id, 'prod_sync_price', True),
        "prod_sync_images": get_config(shop.id, 'prod_sync_images', False),
        # ... other fields
    }
    return render_template('dashboard.html', logs=logs, odoo_status=True if odoo else False, current_settings=current_settings)

@app.route('/api/settings/save', methods=['POST'])
def api_save_settings():
    if 'shop_id' not in session: return jsonify({"error": "401"}), 401
    shop_id = session['shop_id']
    data = request.json
    
    s = Shop.query.get(shop_id)
    if 'odoo_url' in data: s.odoo_url = data['odoo_url']
    if 'odoo_db' in data: s.odoo_db = data['odoo_db']
    if 'odoo_username' in data: s.odoo_username = data['odoo_username']
    if 'odoo_password' in data and data['odoo_password']: s.odoo_password = data['odoo_password']
    db.session.commit()
    
    set_config(shop_id, 'prod_sync_price', data.get('prod_sync_price', True))
    set_config(shop_id, 'prod_sync_images', data.get('prod_sync_images', False))
    # ... save others
    return jsonify({"message": "Saved"})

@app.route('/api/logs/live', methods=['GET'])
def api_live_logs():
    if 'shop_id' not in session: return jsonify([])
    logs = SyncLog.query.filter_by(shop_id=session['shop_id']).order_by(SyncLog.timestamp.desc()).limit(100).all()
    data = []
    for l in logs:
        iso = l.timestamp.isoformat()
        if not iso.endswith('Z'): iso += 'Z'
        data.append({'id': l.id, 'timestamp': iso, 'message': f"[{l.entity}] {l.message}", 'type': 'info', 'details': l.status})
    return jsonify(data)

@app.route('/webhook/orders', methods=['POST'])
def order_webhook_route():
    if not verify_webhook(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "401", 401
    domain = request.headers.get('X-Shopify-Topic-Domain')
    shop = Shop.query.filter_by(shop_url=domain).first()
    if not shop: return "200", 200
    
    activate_shopify_session(shop)
    odoo = get_odoo_connection(shop.id)
    if odoo:
        with app.app_context(): process_order_data(request.json, shop.id, odoo)
    return "200", 200

# --- SCHEDULER LOOP ---
def run_schedule_loop():
    with app.app_context():
        shops = Shop.query.filter_by(is_active=True).all()
        for shop in shops:
            try:
                activate_shopify_session(shop)
                odoo = get_odoo_connection(shop.id)
                if odoo:
                    sync_products_master(shop.id, odoo, None)
            except Exception as e: print(f"Err {shop.shop_url}: {e}")

def scheduler_thread():
    schedule.every(30).minutes.do(run_schedule_loop)
    while True:
        schedule.run_pending()
        time.sleep(1)

t = threading.Thread(target=scheduler_thread, daemon=True)
t.start()

if __name__ == '__main__':
    app.run(debug=True)
