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
from datetime import datetime, timedelta

# --- MONKEY PATCH: FORCE SHOPIFY TO ACCEPT NEW SCOPES ---
class PermissiveApiAccess(shopify.api_access.ApiAccess):
    def __init__(self, scopes):
        if isinstance(scopes, str):
            self._scopes = set(scope.strip() for scope in scopes.split(","))
        else:
            self._scopes = set(scopes)
    def __iter__(self): return iter(self._scopes)
    def __str__(self): return ", ".join(self._scopes)

shopify.api_access.ApiAccess = PermissiveApiAccess
shopify.session.ApiAccess = PermissiveApiAccess
# --------------------------------------------------------

app = Flask(__name__)
# USE A STATIC SECRET KEY IN PRODUCTION!
app.secret_key = os.getenv('SECRET_KEY', 'dev_secret_key_change_me_in_prod')

# --- CONFIG ---
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_SECRET = os.getenv('SHOPIFY_SECRET')
APP_URL = os.getenv('APP_URL')
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///local.db')

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+pg8000://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
shopify.Session.setup(api_key=SHOPIFY_API_KEY, secret=SHOPIFY_SECRET)

# --- HELPERS ---
def get_shop_config(shop_id, key, default=None):
    with app.app_context():
        setting = AppSetting.query.filter_by(shop_id=shop_id, key=key).first()
        if not setting: return default
        try: return json.loads(setting.value)
        except: return setting.value

def set_shop_config(shop_id, key, value):
    try:
        setting = AppSetting.query.filter_by(shop_id=shop_id, key=key).first()
        if not setting:
            setting = AppSetting(shop_id=shop_id, key=key)
            db.session.add(setting)
        # Store booleans/lists as JSON strings
        setting.value = json.dumps(value)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print(f"Config Save Error: {e}")

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
        # Shorten message if too long
        msg = str(message)[:500]
        log = SyncLog(shop_id=shop_id, entity=entity, status=status, message=msg, timestamp=datetime.utcnow())
        db.session.add(log)
        db.session.commit()
    except:
        db.session.rollback()

def extract_id(res):
    if isinstance(res, list) and len(res) > 0: return res[0]
    return res

# --- CORE LOGIC: ORDERS ---
def process_order_data(data, shop, odoo):
    """
    Syncs a Shopify Order to Odoo.
    Returns: (Success Boolean, Message String)
    """
    try:
        shopify_name = data.get('name')
        client_ref = f"ONLINE_{shopify_name}" # Unique Reference
        company_id = shop.odoo_company_id
        
        # 1. Check if Order Exists in Odoo
        # We search by client_order_ref to ensure uniqueness
        existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
            'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
        
        existing_order_id = existing_ids[0] if existing_ids else None
        
        # 2. Update Logic: Only update if state is Draft/Sent
        if existing_order_id:
            info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                'sale.order', 'read', [[existing_order_id]], {'fields': ['state']})
            current_state = info[0]['state']
            
            if current_state not in ['draft', 'sent']:
                return True, f"Skipped Update: Order {shopify_name} is already {current_state} in Odoo."
        
        # 3. Customer / Partner Handling
        email = data.get('email') or data.get('contact_email')
        partner = odoo.search_partner_by_email(email)
        
        if not partner:
            # Create new partner
            cust = data.get('customer', {})
            addr = data.get('billing_address') or {}
            vat = None
            # Extract VAT if present in attributes
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
        
        # Use parent ID if it's a contact
        partner_id = extract_id(partner.get('parent_id') or partner.get('id'))

        # Helper for Addresses (Invoice/Shipping)
        def get_child(addr_data, type_val):
            if not addr_data: return partner_id
            name = addr_data.get('name') or partner.get('name', 'Customer')
            return odoo.find_or_create_child_address(partner_id, {
                'name': name, 'street': addr_data.get('address1'), 'city': addr_data.get('city'),
                'zip': addr_data.get('zip'), 'country_code': addr_data.get('country_code'),
                'phone': addr_data.get('phone'), 'email': email
            }, type_val)

        invoice_id = get_child(data.get('billing_address'), 'invoice')
        shipping_id = get_child(data.get('shipping_address'), 'delivery')
        user_id = odoo.get_partner_salesperson(partner_id) or odoo.uid

        # 4. Build Order Lines
        lines = []
        for item in data.get('line_items', []):
            sku = item.get('sku')
            if not sku: continue # Skip items without SKU
            
            # Find Product
            pid = odoo.search_product_by_sku(sku, company_id)
            if not pid:
                # Optional: Auto-create product if missing (disabled for safety, enabled if preferred)
                # odoo.create_product(...) 
                log_event(shop.id, 'Product', 'Warning', f"Product {sku} not found. Skipping line.")
                continue
            
            price = float(item['price'])
            qty = int(item['quantity'])
            
            # Calculate discount percentage if exists
            disc_amount = float(item.get('total_discount', 0))
            line_total = price * qty
            pct = (disc_amount / line_total) * 100 if line_total > 0 else 0.0

            lines.append((0,0, {
                'product_id': pid, 
                'product_uom_qty': qty, 
                'price_unit': price, 
                'name': item['name'], 
                'discount': pct
            }))

        # 5. Shipping Lines (Exact Name Match)
        for ship in data.get('shipping_lines', []):
            cost = float(ship.get('price', 0.0))
            title = ship.get('title', 'Shipping')
            
            # Try to find service with exact name
            spid = odoo.search_product_by_name(title, company_id)
            if not spid:
                # Fallback to generic
                spid = odoo.search_product_by_name("Shopify Shipping", company_id)
                
            if not spid:
                # Create generic if completely missing
                odoo.create_service_product("Shopify Shipping", company_id)
                spid = odoo.search_product_by_name("Shopify Shipping", company_id)

            if spid:
                lines.append((0,0, {
                    'product_id': spid, 
                    'product_uom_qty': 1, 
                    'price_unit': cost, 
                    'name': title, 
                    'discount': 0.0
                }))

        if not lines: 
            return False, "No valid lines found (Check SKUs)"

        # 6. Payment Method in Notes
        gateway = data.get('gateway', 'Unknown')
        note = f"Shopify Order: {shopify_name}\nPayment Method: {gateway}"
        
        vals = {
            'partner_id': partner_id, 
            'partner_invoice_id': invoice_id, 
            'partner_shipping_id': shipping_id,
            'order_line': lines, 
            'user_id': user_id, 
            'note': note
        }
        if company_id: vals['company_id'] = int(company_id)

        # 7. Execute Create or Update
        if existing_order_id:
            # Replace lines (5,0,0) removes all existing links
            vals['order_line'] = [(5,0,0)] + lines
            odoo.update_sale_order(existing_order_id, vals)
            log_event(shop.id, 'Order', 'Success', f"Updated {shopify_name}")
            return True, f"Updated {shopify_name}"
        else:
            vals['name'] = client_ref
            vals['client_order_ref'] = client_ref
            vals['state'] = 'draft' # Always create as Quotation
            
            odoo.create_sale_order(vals, context={'manual_price': True})
            log_event(shop.id, 'Order', 'Success', f"Created {shopify_name}")
            return True, f"Created {shopify_name}"

    except Exception as e:
        log_event(shop.id, 'Order', 'Error', f"{data.get('name')}: {str(e)}")
        return False, str(e)


# --- API ROUTES ---

@app.route('/api/get_settings', methods=['GET'])
def get_settings():
    shop_url = request.args.get('shop_url')
    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop: return jsonify({})

    # List of keys managed by AppSetting (not Shop columns)
    config_keys = [
        'inventory_field', 'sync_zero_stock', 'inventory_locations', 
        'prod_sync_price', 'prod_sync_title', 'prod_sync_desc', 
        'prod_sync_images', 'prod_auto_create'
    ]
    
    data = {
        'odoo_url': shop.odoo_url or '',
        'odoo_db': shop.odoo_db or '',
        'odoo_username': shop.odoo_username or '',
        'odoo_company_id': shop.odoo_company_id or '',
        'has_password': bool(shop.odoo_password) 
    }
    
    for k in config_keys:
        val = get_shop_config(shop.id, k)
        # Ensure we send proper types (booleans as booleans, not strings)
        data[k] = val
        
    return jsonify(data)

@app.route('/api/save_settings', methods=['POST'])
def save_settings():
    data = request.json
    shop_url = data.get('shop_url')
    if not shop_url: return jsonify({'error': 'Missing shop_url'}), 400

    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop: return jsonify({'error': 'Shop not found'}), 404
    
    # Save Shop Credentials
    if 'odoo_url' in data: shop.odoo_url = data['odoo_url']
    if 'odoo_db' in data: shop.odoo_db = data['odoo_db']
    if 'odoo_username' in data: shop.odoo_username = data['odoo_username']
    if 'odoo_company_id' in data: shop.odoo_company_id = data['odoo_company_id']
    if data.get('odoo_password'): shop.odoo_password = data['odoo_password']
    
    db.session.commit()
    
    # Save Configs
    config_keys = [
        'inventory_field', 'sync_zero_stock', 'inventory_locations', 
        'prod_sync_price', 'prod_sync_title', 'prod_sync_desc', 
        'prod_sync_images', 'prod_auto_create'
    ]
    for key in config_keys:
        if key in data:
            set_shop_config(shop.id, key, data[key])

    return jsonify({'message': 'Settings Saved Successfully'})

@app.route('/api/connection/test', methods=['POST'])
def test_connection():
    data = request.json
    shop_url = data.get('shop_url')
    shop = Shop.query.filter_by(shop_url=shop_url).first()
    
    if not shop: return jsonify({'error': 'Shop not found'}), 404
    
    try:
        odoo = get_odoo_connection(shop)
        if odoo and odoo.uid:
            log_event(shop.id, 'Connection', 'Success', 'Manual Health Check Passed')
            return jsonify({'message': f'Connection Healthy! (UID: {odoo.uid})', 'status': 'ok'})
        else:
            return jsonify({'error': 'Authentication Failed', 'status': 'error'}), 400
    except Exception as e:
        return jsonify({'error': str(e), 'status': 'error'}), 500

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
            if not order: return jsonify({'error': 'Order not found'}), 404
            
            success, msg = process_order_data(order.to_dict(), shop, odoo)
            
            if success: return jsonify({'message': msg})
            else: return jsonify({'error': msg}), 400
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# [Keep existing Webhooks, Logs, and Scheduler code as is from previous file...]
# ... (Include the rest of the file: get_recent_orders, webhooks, scheduler, etc.)
# For brevity, assuming standard imports and functions like get_recent_orders match previous logic 
# but rely on process_order_data for the heavy lifting.

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
                log = SyncLog.query.filter(SyncLog.shop_id == shop.id, SyncLog.message.contains(o.name)).order_by(SyncLog.id.desc()).first()
                status = 'Pending'
                if log:
                    if 'Success' in log.status: status = 'Synced'
                    elif 'Error' in log.status: status = 'Error'
                
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

@app.route('/api/logs/live', methods=['GET'])
def api_live_logs():
    shop_url = request.args.get('shop_url')
    if not shop_url: return jsonify([])
    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop: return jsonify([])
    
    logs = SyncLog.query.filter_by(shop_id=shop.id).order_by(SyncLog.id.desc()).limit(50).all()
    # Format for the React Frontend
    return jsonify([{'id': l.id, 'timestamp': l.timestamp.isoformat(), 'message': f"[{l.entity}] {l.message}", 'type': l.status.lower()} for l in logs])

# Standard boilerplate for index, auth, callback... (Keep unchanged)
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
    return redirect(url_for('index', shop=shop_url))

if __name__ == '__main__':
    with app.app_context(): db.create_all()
    app.run(debug=True)
