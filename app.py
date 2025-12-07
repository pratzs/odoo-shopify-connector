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
# Fix for Supabase URL compatibility
database_url = os.getenv('DATABASE_URL', 'sqlite:///local.db')
if database_url and database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Parse Odoo Locations (e.g. "12,15")
location_env = os.getenv('ODOO_STOCK_LOCATION_IDS', '0')
try:
    ODOO_LOCATION_IDS = [int(x) for x in location_env.split(',') if x.strip().isdigit()]
except:
    ODOO_LOCATION_IDS = []

SHOPIFY_LOCATION_ID = int(os.getenv('SHOPIFY_WAREHOUSE_ID', '0'))

db.init_app(app)

# Initialize Odoo Connection
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

# Create DB Tables
with app.app_context():
    try: db.create_all()
    except: pass

# --- HELPER FUNCTIONS ---
def verify_shopify(data, hmac_header):
    secret = os.getenv('SHOPIFY_SECRET')
    if not secret: return True 
    if not hmac_header: return False
    digest = hmac.new(secret.encode('utf-8'), data, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), hmac_header)

def build_order_lines(data):
    """Parses Shopify JSON to create Odoo Order Lines"""
    lines = []
    
    # 1. Product Lines
    for item in data.get('line_items', []):
        sku = item.get('sku')
        if not sku: continue
        
        product_id = odoo.search_product_by_sku(sku)
        if product_id:
            price = float(item.get('price', 0))
            qty = int(item.get('quantity', 1))
            
            # Calculate Discount %
            discount_amount = float(item.get('total_discount', 0))
            discount_percent = 0.0
            if price > 0 and qty > 0 and discount_amount > 0:
                discount_percent = (discount_amount / (price * qty)) * 100
            
            lines.append((0, 0, {
                'product_id': product_id,
                'product_uom_qty': qty,
                'price_unit': price,
                'name': item['name'],
                'discount': discount_percent
            }))
            
    # 2. Shipping Lines (Match by Name)
    for ship in data.get('shipping_lines', []):
        cost = float(ship.get('price', 0))
        title = ship.get('title', 'Shipping')
        
        # Try to find exact shipping product
        shipping_product_id = odoo.search_product_by_name(title)
        
        # Fallback to generic if specific name not found
        if not shipping_product_id:
            shipping_product_id = odoo.search_product_by_name("Shipping")
        
        if cost >= 0 and shipping_product_id:
            lines.append((0, 0, {
                'product_id': shipping_product_id,
                'product_uom_qty': 1,
                'price_unit': cost,
                'name': title,
                'is_delivery': True
            }))
            
    return lines

# --- ROUTES ---

@app.route('/')
def dashboard():
    logs = []
    try: logs = SyncLog.query.order_by(SyncLog.timestamp.desc()).limit(20).all()
    except: pass
    odoo_status = True if odoo else False
    return render_template('dashboard.html', logs=logs, odoo_status=odoo_status, locations=ODOO_LOCATION_IDS)

@app.route('/test/simulate_order', methods=['POST'])
def simulate_order():
    if not odoo: return jsonify({"message": "Odoo Offline"}), 500
    try:
        test_email = os.getenv('ODOO_USERNAME') 
        partner = odoo.search_partner_by_email(test_email)
        if partner:
            msg = f"Connection Successful! Found Admin Partner ID: {partner['id']}"
            status = 'Success'
        else:
            msg = f"Connected, but Admin email {test_email} not found in Contacts."
            status = 'Warning'
            
        log = SyncLog(entity='Test Connection', status=status, message=msg)
        db.session.add(log)
        db.session.commit()
        return jsonify({"message": msg})
    except Exception as e:
        return jsonify({"message": f"Test Failed: {str(e)}"}), 500

# --- JOB 1: ORDER CREATE & UPDATE ---
@app.route('/webhook/orders', methods=['POST'])
@app.route('/webhook/orders/updated', methods=['POST'])
def order_webhook():
    if not odoo: return "Offline", 500
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')):
        return "Unauthorized", 401
    
    data = request.json
    email = data.get('email')
    # Custom Prefix Logic
    client_ref = f"ONLINE_{data.get('name')}"
    
    # 1. Check if order exists
    order_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
        'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
    
    # 2. Find Customer (Parent/Child Logic)
    partner = odoo.search_partner_by_email(email)
    if not partner:
        log = SyncLog(entity='Order', status='Skipped', message=f"Customer {email} not found")
        db.session.add(log)
        db.session.commit()
        return "Skipped", 200
    
    if partner.get('parent_id'):
        invoice_id = partner['parent_id'][0] # Bill to Parent
        shipping_id = partner['id']          # Ship to Child
        main_id = invoice_id
    else:
        invoice_id = shipping_id = main_id = partner['id']

    lines = build_order_lines(data)
    if not lines: return "No Lines", 200

    # 3. Compile Note
    notes = []
    if data.get('note'): notes.append(f"Customer Note: {data.get('note')}")
    if data.get('payment_gateway_names'): notes.append(f"Payment: {', '.join(data['payment_gateway_names'])}")
    final_note = "\n".join(notes)

    if order_ids:
        # UPDATE EXISTING ORDER (If in Draft)
        existing = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'sale.order', 'read', [order_ids[0]], {'fields': ['state']})
        
        if existing and existing[0]['state'] in ['draft', 'sent']:
            # Replace lines and update note
            odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'write', [[order_ids[0]], {
                'order_line': [(5, 0, 0)] + lines, 
                'note': final_note
            }])
            log = SyncLog(entity='Order Update', status='Success', message=f"Updated {client_ref}")
        else:
            # Order is locked, just post a message
            odoo.post_message(order_ids[0], "Shopify Order Updated (Locked in Odoo)")
            log = SyncLog(entity='Order Update', status='Ignored', message=f"{client_ref} is locked")
    else:
        # CREATE NEW ORDER
        try:
            odoo.create_sale_order({
                'name': client_ref, 
                'client_order_ref': client_ref,
                'partner_id': main_id, 
                'partner_invoice_id': invoice_id, 
                'partner_shipping_id': shipping_id,
                'order_line': lines, 
                'user_id': odoo.uid, # Assign to API User
                'state': 'draft', 
                'note': final_note
            })
            log = SyncLog(entity='Order Create', status='Success', message=f"Created {client_ref}")
        except Exception as e:
            log = SyncLog(entity='Order Create', status='Failed', message=str(e))

    db.session.add(log)
    db.session.commit()
    return "Synced", 200

# --- JOB 2: REFUNDS ---
@app.route('/webhook/refunds', methods=['POST'])
def refund_webhook():
    if not odoo: return "Offline", 500
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')):
        return "Unauthorized", 401

    data = request.json
    # We need the Shopify Order ID to find the Odoo Order
    shopify_order_id = data.get('order_id')
    
    # Call Shopify API to get Order Name (e.g. #1001) from ID
    headers = {"X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN')}
    res = requests.get(f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders/{shopify_order_id}.json", headers=headers)
    
    if res.status_code == 200:
        order_name = res.json().get('order', {}).get('name')
        client_ref = f"ONLINE_{order_name}"
        
        # Find Odoo Order
        order_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
            
        if order_ids:
            odoo.post_message(order_ids[0], "EVENT: Refund processed in Shopify.")
            log = SyncLog(entity='Refund', status='Success', message=f"Logged refund for {client_ref}")
            db.session.add(log)
            db.session.commit()

    return "Refund Logged", 200

# --- JOB 3: CANCELLATIONS (Shopify -> Odoo) ---
@app.route('/webhook/orders/cancelled', methods=['POST'])
def order_cancelled_webhook():
    if not odoo: return "Offline", 500
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')):
        return "Unauthorized", 401

    data = request.json
    client_ref = f"ONLINE_{data.get('name')}"
    
    order_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
        'sale.order', 'search', [[['client_order_ref', '=', client_ref], ['state', '!=', 'cancel']]])

    if order_ids:
        odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'action_cancel', [order_ids])
        log = SyncLog(entity='Order Cancel', status='Success', message=f"Cancelled {client_ref}")
        db.session.add(log)
        db.session.commit()
        
    return "Cancelled", 200

# --- JOB 4: INVENTORY SYNC ---
@app.route('/sync/inventory', methods=['GET'])
def sync_inventory():
    if not odoo: return jsonify({"error": "Offline"}), 500
    
    last_run = datetime.utcnow() - timedelta(minutes=35)
    try: product_ids = odoo.get_changed_products(str(last_run))
    except: return jsonify({"error": "Read Failed"}), 500
    
    count = 0
    # Shopify API Setup
    shopify_url = f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10"
    headers = {"X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN'), "Content-Type": "application/json"}

    for p_id in product_ids:
        total_qty = odoo.get_total_qty_for_locations(p_id, ODOO_LOCATION_IDS)
        
        # Get SKU
        p_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'product.product', 'read', [p_id], {'fields': ['default_code']})
        sku = p_data[0].get('default_code')
        
        if sku:
            # In Production: Query Shopify for InventoryItem ID using SKU, then Post Update
            # For now, we log it
            print(f"SYNC: SKU {sku} Total: {total_qty} -> Shopify")
            count += 1
            
    return jsonify({"synced": count})

# --- JOB 5: ODOO STATUS SYNC (Odoo -> Shopify) ---
@app.route('/sync/order_status', methods=['GET'])
def sync_order_status():
    if not odoo: return jsonify({"error": "Offline"}), 500
    
    last_run = datetime.utcnow() - timedelta(minutes=35)
    # Find Cancelled Orders in Odoo
    domain = [('write_date', '>', str(last_run)), ('state', '=', 'cancel')]
    cancelled_orders = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
        'sale.order', 'search_read', [domain], {'fields': ['client_order_ref']})
    
    count = 0
    headers = {"X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN')}
    
    for order in cancelled_orders:
        ref = order.get('client_order_ref', '')
        if ref.startswith('ONLINE_#'):
            shopify_name = ref.replace('ONLINE_', '')
            
            # Find Shopify ID
            search = requests.get(f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders.json?name={shopify_name}&status=open", headers=headers)
            if search.status_code == 200 and search.json().get('orders'):
                shopify_id = search.json()['orders'][0]['id']
                # Cancel in Shopify
                requests.post(f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders/{shopify_id}/cancel.json", headers=headers)
                
                log = SyncLog(entity='Status Sync', status='Success', message=f"Cancelled Shopify Order {shopify_name}")
                db.session.add(log)
                db.session.commit()
                count += 1

    return jsonify({"cancelled_syncs": count})

if __name__ == '__main__':
    app.run(debug=True)
