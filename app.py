import os
import hmac
import hashlib
import base64
import json
from flask import Flask, request, jsonify, render_template
from models import db, ProductMap, SyncLog, AppSetting
from odoo_client import OdooClient
import requests
from datetime import datetime, timedelta
import random

app = Flask(__name__)

# --- CONFIGURATION ---
database_url = os.getenv('DATABASE_URL', 'sqlite:///local.db')
if database_url:
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+pg8000://", 1)
    elif database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+pg8000://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

SHOPIFY_LOCATION_ID = int(os.getenv('SHOPIFY_WAREHOUSE_ID', '0'))

db.init_app(app)

odoo = None
try:
    odoo = OdooClient(
        url=os.getenv('ODOO_URL'),
        db=os.getenv('ODOO_DB'),
        username=os.getenv('ODOO_USERNAME'),
        password=os.getenv('ODOO_PASSWORD')
    )
except Exception as e:
    print(f"Odoo Startup Error: {e}")

with app.app_context():
    try: db.create_all()
    except: pass

# --- HELPERS ---
def get_config(key, default=None):
    try:
        setting = AppSetting.query.get(key)
        try: return json.loads(setting.value)
        except: return setting.value
    except: return default

def set_config(key, value):
    try:
        setting = AppSetting.query.get(key)
        if not setting:
            setting = AppSetting(key=key)
            db.session.add(setting)
        setting.value = json.dumps(value)
        db.session.commit()
        return True
    except: return False

def verify_shopify(data, hmac_header):
    secret = os.getenv('SHOPIFY_SECRET')
    if not secret: return True 
    if not hmac_header: return False
    digest = hmac.new(secret.encode('utf-8'), data, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), hmac_header)

def log_event(entity, status, message):
    try:
        log = SyncLog(entity=entity, status=status, message=message)
        db.session.add(log)
        db.session.commit()
    except Exception as e: print(f"DB LOG ERROR: {e}")

def process_order_data(data):
    """Core logic to sync a single order"""
    email = data.get('email') or data.get('contact_email')
    shopify_name = data.get('name')
    client_ref = f"ONLINE_{shopify_name}"
    company_id = get_config('odoo_company_id')
    
    # 1. Check if Order Exists
    try:
        existing = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
        if existing: return True, f"Order {client_ref} exists."
    except Exception as e: return False, str(e)

    # 2. CUSTOMER LOGIC (RESTORED & ENHANCED)
    partner = odoo.search_partner_by_email(email)
    
    # IF CUSTOMER MISSING -> CREATE NEW
    if not partner:
        cust_data = data.get('customer', {})
        def_address = data.get('billing_address') or data.get('shipping_address') or {}
        
        # Name fallback logic
        name = f"{cust_data.get('first_name', '')} {cust_data.get('last_name', '')}".strip()
        if not name: name = def_address.get('name') or email
            
        new_partner_vals = {
            'name': name,
            'email': email,
            'phone': cust_data.get('phone') or def_address.get('phone'),
            'company_type': 'company', # Create as Parent Company
            'street': def_address.get('address1'),
            'city': def_address.get('city'),
            'zip': def_address.get('zip'),
            'country_code': def_address.get('country_code'),
        }
        
        # Ensure new customer belongs to the correct Odoo Company
        if company_id:
            new_partner_vals['company_id'] = int(company_id)

        try:
            partner_id = odoo.create_partner(new_partner_vals)
            # Create a minimal partner object so code continues flow
            partner = {'id': partner_id, 'name': name, 'parent_id': False}
            log_event('Customer', 'Success', f"Created New Customer: {name}")
        except Exception as e:
            log_event('Customer', 'Error', f"Failed to create customer {email}: {e}")
            return False, f"Customer Creation Failed: {e}"

    # Determine Parent/Child IDs
    if partner.get('parent_id'):
        partner_id = partner['parent_id'][0] # Use Parent
    else:
        partner_id = partner['id'] # Is Parent

    # Handle Delivery Address (Child Contact)
    ship_addr = data.get('shipping_address', {})
    if ship_addr:
        shipping_data = {
            'name': f"{ship_addr.get('first_name', '')} {ship_addr.get('last_name', '')}".strip(),
            'street': ship_addr.get('address1'),
            'city': ship_addr.get('city'),
            'zip': ship_addr.get('zip'),
            'phone': ship_addr.get('phone'),
            'country_code': ship_addr.get('country_code'),
            'email': email
        }
        try:
            # Find existing child address or create new one linked to partner_id
            shipping_id = odoo.find_or_create_child_address(partner_id, shipping_data, type='delivery')
        except Exception as e:
            print(f"Address Warning: {e}")
            shipping_id = partner_id # Fallback to parent if address creation fails
    else:
        shipping_id = partner_id

    invoice_id = partner_id # Bill to Parent

    # 3. Build Lines
    lines = []
    for item in data.get('line_items', []):
        sku = item.get('sku')
        if not sku: continue
        product_id = odoo.search_product_by_sku(sku, company_id)
        if product_id:
            price = float(item.get('price', 0))
            qty = int(item.get('quantity', 1))
            disc = float(item.get('total_discount', 0))
            pct = (disc / (price * qty)) * 100 if price > 0 else 0.0
            lines.append((0, 0, {'product_id': product_id, 'product_uom_qty': qty, 'price_unit': price, 'name': item['name'], 'discount': pct}))
        else:
            log_event('Product', 'Warning', f"SKU {item.get('sku')} not found in Company {company_id}")

    for ship in data.get('shipping_lines', []):
        cost = float(ship.get('price', 0))
        ship_pid = odoo.search_product_by_name(ship.get('title'), company_id) or odoo.search_product_by_name("Shipping", company_id)
        if not ship_pid:
            try:
                ship_pid = odoo.create_service_product(ship.get('title'), company_id)
                if isinstance(ship_pid, list): ship_pid = ship_pid[0]
            except: pass
        if cost >= 0 and ship_pid:
            lines.append((0, 0, {'product_id': ship_pid, 'product_uom_qty': 1, 'price_unit': cost, 'name': ship.get('title'), 'is_delivery': True}))

    if not lines: return False, "No valid lines"
    
    notes = [f"Note: {data.get('note', '')}"]
    if data.get('payment_gateway_names'): notes.append(f"Payment: {', '.join(data['payment_gateway_names'])}")
    elif data.get('gateway'): notes.append(f"Payment: {data['gateway']}")
    
    try:
        vals = {
            'name': client_ref, 'client_order_ref': client_ref,
            'partner_id': partner_id, 'partner_invoice_id': invoice_id, 'partner_shipping_id': shipping_id,
            'order_line': lines, 'user_id': odoo.uid, 'state': 'draft', 'note': "\n\n".join(notes)
        }
        if company_id: vals['company_id'] = int(company_id)
        
        odoo.create_sale_order(vals)
        log_event('Order', 'Success', f"Synced {client_ref}")
        return True, "Synced"
    except Exception as e:
        log_event('Order', 'Error', str(e))
        return False, str(e)

# --- ROUTES ---

@app.route('/')
def dashboard():
    try:
        logs_orders = SyncLog.query.filter(SyncLog.entity.in_(['Order', 'Order Cancel'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_inventory = SyncLog.query.filter_by(entity='Inventory').order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_customers = SyncLog.query.filter_by(entity='Customer').order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_system = SyncLog.query.filter(SyncLog.entity.notin_(['Order', 'Order Cancel', 'Inventory', 'Customer'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
    except:
        logs_orders = logs_inventory = logs_customers = logs_system = []
    
    current_settings = {
        "locations": get_config('inventory_locations', []),
        "field": get_config('inventory_field', 'qty_available'),
        "sync_zero": get_config('sync_zero_stock', False),
        "combine_committed": get_config('combine_committed', False),
        "company_id": get_config('odoo_company_id', None),
        "cust_direction": get_config('cust_direction', 'bidirectional'),
        "cust_auto_sync": get_config('cust_auto_sync', True)
    }

    odoo_status = True if odoo else False
    return render_template('dashboard.html', 
                           logs_orders=logs_orders, logs_inventory=logs_inventory, 
                           logs_customers=logs_customers, logs_system=logs_system,
                           odoo_status=odoo_status, current_settings=current_settings)

@app.route('/api/odoo/companies', methods=['GET'])
def api_get_companies():
    if not odoo: return jsonify({"error": "Odoo Offline"}), 500
    try: return jsonify(odoo.get_companies())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/api/odoo/locations', methods=['GET'])
def api_get_locations():
    if not odoo: return jsonify({"error": "Odoo Offline"}), 500
    try:
        company_id = request.args.get('company_id')
        return jsonify(odoo.get_locations(company_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/settings/save', methods=['POST'])
def api_save_settings():
    data = request.json
    set_config('inventory_locations', data.get('locations', []))
    set_config('inventory_field', data.get('field', 'qty_available'))
    set_config('sync_zero_stock', data.get('sync_zero', False))
    set_config('combine_committed', data.get('combine_committed', False))
    set_config('odoo_company_id', data.get('company_id'))
    # Save Customer Settings
    set_config('cust_direction', data.get('cust_direction'))
    set_config('cust_auto_sync', data.get('cust_auto_sync'))
    return jsonify({"message": "Settings Saved"})

@app.route('/sync/inventory', methods=['GET'])
def sync_inventory():
    if not odoo: return jsonify({"error": "Offline"}), 500
    target_locations = get_config('inventory_locations', [])
    target_field = get_config('inventory_field', 'qty_available')
    sync_zero = get_config('sync_zero_stock', False)
    company_id = get_config('odoo_company_id', None)
    
    if not company_id:
        try:
            u = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'res.users', 'read', [[odoo.uid]], {'fields': ['company_id']})
            if u: company_id = u[0]['company_id'][0]
        except: pass

    last_run = datetime.utcnow() - timedelta(minutes=35)
    try: product_ids = odoo.get_changed_products(str(last_run), company_id)
    except: return jsonify({"error": "Read Failed"}), 500
    
    count = 0
    for p_id in product_ids:
        total = odoo.get_total_qty_for_locations(p_id, target_locations, field_name=target_field)
        if sync_zero and total <= 0: continue
        count += 1
        if count <= 3:
             p_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'read', [p_id], {'fields': ['default_code']})
             sku = p_data[0].get('default_code')
             log_event('Inventory', 'Info', f"Synced SKU {sku}: {total}")
    return jsonify({"synced": count})

@app.route('/sync/orders/manual', methods=['GET'])
def manual_order_fetch():
    url = f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders.json?status=open&limit=10"
    headers = {"X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN')}
    res = requests.get(url, headers=headers)
    orders = res.json().get('orders', []) if res.status_code == 200 else []
    mapped_orders = []
    for o in orders:
        status = "Not Synced"
        try:
            exists = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search', [[['client_order_ref', 'ilike', o['name']]]])
            if exists: status = "Synced"
        except: pass
        if o.get('cancelled_at'): status = "Cancelled (Skipped)"
        mapped_orders.append({'id': o['id'], 'name': o['name'], 'date': o['created_at'], 'total': o['total_price'], 'odoo_status': status})
    return jsonify({"orders": mapped_orders})

@app.route('/sync/orders/import_batch', methods=['POST'])
def import_selected_orders():
    ids = request.json.get('order_ids', [])
    headers = {"X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN')}
    synced = 0
    for oid in ids:
        res = requests.get(f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders/{oid}.json", headers=headers)
        if res.status_code == 200:
            success, _ = process_order_data(res.json().get('order'))
            if success: synced += 1
    return jsonify({"message": f"Batch Complete. Synced: {synced}"})

@app.route('/webhook/orders', methods=['POST'])
@app.route('/webhook/orders/updated', methods=['POST'])
def order_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    process_order_data(request.json)
    return "Received", 200

@app.route('/webhook/orders/cancelled', methods=['POST'])
def order_cancelled_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    data = request.json
    client_ref = f"ONLINE_{data.get('name')}"
    order_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search', [[['client_order_ref', '=', client_ref], ['state', '!=', 'cancel']]])
    if order_ids:
        odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'action_cancel', [order_ids])
        log_event('Order Cancel', 'Success', f"Cancelled {client_ref}")
    return "Cancelled", 200

@app.route('/webhook/refunds', methods=['POST'])
def refund_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    log_event('Refund', 'Info', "Refund webhook received")
    return "Received", 200

@app.route('/test/simulate_order', methods=['POST'])
def test_sim_dummy(): return jsonify({})

@app.route('/sync/order_status', methods=['GET'])
def sync_order_status(): return jsonify({"status": "Checked"})

if __name__ == '__main__':
    app.run(debug=True)
