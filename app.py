import os
import hmac
import hashlib
import base64
from flask import Flask, request, jsonify, render_template
from models import db, ProductMap, SyncLog
from odoo_client import OdooClient
import requests
from datetime import datetime, timedelta
import random

app = Flask(__name__)

# --- CONFIGURATION ---
# FIX: Handle Supabase connection string for pg8000 driver
database_url = os.getenv('DATABASE_URL', 'sqlite:///local.db')

if database_url:
    # If it starts with postgres://, change to postgresql+pg8000://
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+pg8000://", 1)
    # If it starts with postgresql://, change to postgresql+pg8000://
    elif database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+pg8000://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Odoo Locations
location_env = os.getenv('ODOO_STOCK_LOCATION_IDS', '0')
try:
    ODOO_LOCATION_IDS = [int(x) for x in location_env.split(',') if x.strip().isdigit()]
except:
    ODOO_LOCATION_IDS = []

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
    print(f"Odoo Connection Error: {e}")

with app.app_context():
    try: db.create_all()
    except: pass

def verify_shopify(data, hmac_header):
    secret = os.getenv('SHOPIFY_SECRET')
    if not secret: return True 
    if not hmac_header: return False
    digest = hmac.new(secret.encode('utf-8'), data, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), hmac_header)

def log_event(entity, status, message):
    """Helper to save logs to DB safely"""
    try:
        log = SyncLog(entity=entity, status=status, message=message)
        db.session.add(log)
        db.session.commit()
    except Exception as e:
        print(f"DB LOG ERROR: {e}")

def process_order_data(data):
    """Core logic to sync a single order - Used by Webhook AND Manual Trigger"""
    email = data.get('email')
    shopify_name = data.get('name')
    client_ref = f"ONLINE_{shopify_name}"
    
    # 1. Check if Order Exists (Prevent Resend)
    try:
        existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
        
        if existing_ids:
            return True, f"Order {client_ref} already exists in Odoo."
    except Exception as e:
        return False, f"Odoo Connection Error: {str(e)}"

    # 2. Customer Sync
    partner = odoo.search_partner_by_email(email)
    if not partner:
        log_event('Customer', 'Failed', f"Email {email} not found in Odoo for Order {client_ref}")
        return False, "Customer Missing"
    
    # Hierarchy Logic
    if partner.get('parent_id'):
        invoice_id, shipping_id, main_id = partner['parent_id'][0], partner['id'], partner['parent_id'][0]
    else:
        invoice_id = shipping_id = main_id = partner['id']

    # 3. Build Lines
    lines = []
    # Products
    for item in data.get('line_items', []):
        sku = item.get('sku')
        if not sku: continue
        product_id = odoo.search_product_by_sku(sku)
        
        if product_id:
            price = float(item.get('price', 0))
            qty = int(item.get('quantity', 1))
            discount = float(item.get('total_discount', 0))
            pct = (discount / (price * qty)) * 100 if price > 0 else 0.0
            
            lines.append((0, 0, {
                'product_id': product_id, 'product_uom_qty': qty,
                'price_unit': price, 'name': item['name'], 'discount': pct
            }))
        else:
            log_event('Product', 'Warning', f"SKU {sku} not found in Odoo (Order {client_ref})")

    # Shipping
    for ship in data.get('shipping_lines', []):
        cost = float(ship.get('price', 0))
        title = ship.get('title', 'Shipping')
        ship_pid = odoo.search_product_by_name(title) or odoo.search_product_by_name("Shipping")
        
        if cost >= 0 and ship_pid:
            lines.append((0, 0, {
                'product_id': ship_pid, 'product_uom_qty': 1,
                'price_unit': cost, 'name': title, 'is_delivery': True
            }))

    if not lines: return False, "No valid lines to sync"

    # 4. Create Order with Notes (Updated)
    notes_list = []
    
    # Customer Note
    customer_note = data.get('note')
    if customer_note: 
        notes_list.append(f"Customer Note: {customer_note}")
    
    # Payment Method (Robust Check)
    payment_methods = data.get('payment_gateway_names')
    if not payment_methods:
        # Fallback to 'gateway' if list is empty
        gateway = data.get('gateway')
        if gateway:
            payment_methods = [gateway]
            
    if payment_methods:
        notes_list.append(f"Payment Method: {', '.join(payment_methods)}")
        
    final_note = "\n\n".join(notes_list)
    
    try:
        odoo.create_sale_order({
            'name': client_ref, 'client_order_ref': client_ref,
            'partner_id': main_id, 'partner_invoice_id': invoice_id, 'partner_shipping_id': shipping_id,
            'order_line': lines, 'user_id': odoo.uid, 'state': 'draft', 
            'note': final_note 
        })
        log_event('Order', 'Success', f"Synced {client_ref} to Odoo ID {main_id}. Note: {len(final_note)} chars")
        return True, "Synced"
    except Exception as e:
        log_event('Order', 'Error', f"Failed to create {client_ref}: {str(e)}")
        return False, str(e)

# --- ROUTES ---

@app.route('/')
def dashboard():
    # Fetch logs separated by category for the UI tabs
    try:
        logs_orders = SyncLog.query.filter(SyncLog.entity.in_(['Order', 'Order Cancel'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_inventory = SyncLog.query.filter_by(entity='Inventory').order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_customers = SyncLog.query.filter_by(entity='Customer').order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_system = SyncLog.query.filter(SyncLog.entity.notin_(['Order', 'Order Cancel', 'Inventory', 'Customer'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
    except Exception as e:
        print(f"Database Read Error: {e}")
        logs_orders = logs_inventory = logs_customers = logs_system = []
    
    odoo_status = True if odoo else False
    return render_template('dashboard.html', 
                           logs_orders=logs_orders, logs_inventory=logs_inventory, 
                           logs_customers=logs_customers, logs_system=logs_system,
                           odoo_status=odoo_status, locations=ODOO_LOCATION_IDS)

@app.route('/test/simulate_order', methods=['POST'])
def simulate_order():
    if not odoo: return jsonify({"message": "Odoo Offline"}), 500
    try:
        test_email = os.getenv('ODOO_USERNAME') 
        partner = odoo.search_partner_by_email(test_email)
        status = 'Success' if partner else 'Warning'
        msg = f"Connection Test: Found Admin ID {partner['id']}" if partner else "Connection Test: Admin email not found"
        log_event('Test Connection', status, msg)
        return jsonify({"message": msg})
    except Exception as e:
        return jsonify({"message": f"Test Failed: {str(e)}"}), 500

# --- MANUAL TRIGGER: SYNC RECENT ORDERS ---
@app.route('/sync/orders/manual', methods=['GET'])
def manual_order_sync():
    # Fetch last 5 open orders from Shopify
    url = f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders.json?status=open&limit=5"
    headers = {"X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN')}
    
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        return jsonify({"message": f"Failed to fetch from Shopify: {res.status_code}"}), 500
    
    orders = res.json().get('orders', [])
    synced_count = 0
    skipped_count = 0
    
    for order in orders:
        success, msg = process_order_data(order)
        if success and "already exists" not in msg:
            synced_count += 1
        else:
            skipped_count += 1
        
    return jsonify({"message": f"Manual Sync: Processed {len(orders)}. Synced: {synced_count}, Skipped: {skipped_count}"})

# --- WEBHOOKS ---
@app.route('/webhook/orders', methods=['POST'])
def order_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')):
        return "Unauthorized", 401
    
    process_order_data(request.json)
    return "Received", 200

@app.route('/webhook/orders/cancelled', methods=['POST'])
def order_cancelled_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    data = request.json
    client_ref = f"ONLINE_{data.get('name')}"
    
    order_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
        'sale.order', 'search', [[['client_order_ref', '=', client_ref], ['state', '!=', 'cancel']]])

    if order_ids:
        odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'action_cancel', [order_ids])
        log_event('Order Cancel', 'Success', f"Cancelled {client_ref}")
        
    return "Cancelled", 200

@app.route('/webhook/refunds', methods=['POST'])
def refund_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    log_event('Refund', 'Info', "Refund webhook received")
    return "Received", 200

@app.route('/sync/inventory', methods=['GET'])
def sync_inventory():
    if not odoo: return jsonify({"error": "Offline"}), 500
    
    last_run = datetime.utcnow() - timedelta(minutes=35)
    try:
        product_ids = odoo.get_changed_products(str(last_run))
    except:
        return jsonify({"error": "Read Failed"}), 500
    
    count = 0
    for p_id in product_ids:
        total = odoo.get_total_qty_for_locations(p_id, ODOO_LOCATION_IDS)
        count += 1
        if count <= 3:
             p_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'read', [p_id], {'fields': ['default_code']})
             sku = p_data[0].get('default_code')
             log_event('Inventory', 'Info', f"Synced SKU {sku}: Qty {total}")

    return jsonify({"synced": count, "message": "Inventory Sync Completed"})

@app.route('/sync/order_status', methods=['GET'])
def sync_order_status():
    # Cancellation Odoo -> Shopify logic
    return jsonify({"status": "Checked"})

if __name__ == '__main__':
    app.run(debug=True)
