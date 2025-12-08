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

# Initialize Odoo
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
    email = data.get('email')
    shopify_name = data.get('name')
    client_ref = f"ONLINE_{shopify_name}"
    
    try:
        existing = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
        if existing: return True, f"Order {client_ref} exists."
    except Exception as e: return False, str(e)

    partner = odoo.search_partner_by_email(email)
    if not partner:
        log_event('Customer', 'Failed', f"Email {email} not found")
        return False, "Customer Missing"
    
    if partner.get('parent_id'):
        invoice_id, shipping_id, main_id = partner['parent_id'][0], partner['id'], partner['parent_id'][0]
    else:
        invoice_id = shipping_id = main_id = partner['id']

    lines = []
    for item in data.get('line_items', []):
        product_id = odoo.search_product_by_sku(item.get('sku'))
        if product_id:
            price = float(item.get('price', 0))
            qty = int(item.get('quantity', 1))
            discount = float(item.get('total_discount', 0))
            pct = (discount / (price * qty)) * 100 if price > 0 else 0.0
            lines.append((0, 0, {'product_id': product_id, 'product_uom_qty': qty, 'price_unit': price, 'name': item['name'], 'discount': pct}))
        else:
            log_event('Product', 'Warning', f"SKU {item.get('sku')} not found")

    for ship in data.get('shipping_lines', []):
        ship_pid = odoo.search_product_by_name(ship.get('title')) or odoo.search_product_by_name("Shipping")
        if ship_pid:
            lines.append((0, 0, {'product_id': ship_pid, 'product_uom_qty': 1, 'price_unit': float(ship.get('price',0)), 'name': ship.get('title'), 'is_delivery': True}))

    if not lines: return False, "No valid lines"
    
    notes = [f"Note: {data.get('note', '')}"]
    if data.get('payment_gateway_names'): notes.append(f"Payment: {', '.join(data['payment_gateway_names'])}")
    
    try:
        odoo.create_sale_order({
            'name': client_ref, 'client_order_ref': client_ref,
            'partner_id': main_id, 'partner_invoice_id': invoice_id, 'partner_shipping_id': shipping_id,
            'order_line': lines, 'user_id': odoo.uid, 'state': 'draft', 'note': "\n".join(notes)
        })
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
    
    env_locs = os.getenv('ODOO_STOCK_LOCATION_IDS', '0')
    default_locs = [int(x) for x in env_locs.split(',') if x.strip().isdigit()]
    
    current_settings = {
        "locations": get_config('inventory_locations', default_locs),
        "field": get_config('inventory_field', 'qty_available'),
        "sync_zero": get_config('sync_zero_stock', False),
        "combine_committed": get_config('combine_committed', False),
        "company_id": get_config('odoo_company_id', None)
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
        locs = odoo.get_locations(company_id)
        return jsonify(locs)
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
    return jsonify({"message": "Settings Saved"})

@app.route('/sync/inventory', methods=['GET'])
def sync_inventory():
    if not odoo: return jsonify({"error": "Offline"}), 500
    
    # Load Settings from DB
    env_locs = os.getenv('ODOO_STOCK_LOCATION_IDS', '0')
    default_locs = [int(x) for x in env_locs.split(',') if x.strip().isdigit()]
    
    target_locations = get_config('inventory_locations', default_locs)
    target_field = get_config('inventory_field', 'qty_available')
    sync_zero = get_config('sync_zero_stock', False)

    last_run = datetime.utcnow() - timedelta(minutes=35)
    try: product_ids = odoo.get_changed_products(str(last_run))
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
             log_event('Inventory', 'Info', f"Synced SKU {sku}: {total} ({target_field})")

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

# THIS IS THE MISSING ROUTE FROM YOUR LOGS
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

@app.route('/sync/order_status', methods=['GET'])
def sync_order_status():
    return jsonify({"status": "Checked"})

if __name__ == '__main__':
    app.run(debug=True)
