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
database_url = os.getenv('DATABASE_URL', 'sqlite:///local.db')
if database_url and database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

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
    if not secret: 
        print("DEBUG: No SHOPIFY_SECRET set in env vars.")
        return True 
    
    if not hmac_header:
        print("DEBUG: Request missing X-Shopify-Hmac-Sha256 header")
        return False

    digest = hmac.new(secret.encode('utf-8'), data, hashlib.sha256).digest()
    computed_hmac = base64.b64encode(digest).decode()
    return hmac.compare_digest(computed_hmac, hmac_header)

# --- DASHBOARD (UI) ---
@app.route('/')
def dashboard():
    logs = []
    try:
        logs = SyncLog.query.order_by(SyncLog.timestamp.desc()).limit(20).all()
    except:
        pass
    odoo_status = True if odoo else False
    return render_template('dashboard.html', logs=logs, odoo_status=odoo_status, locations=ODOO_LOCATION_IDS)

# --- SIMULATE TEST ORDER ---
@app.route('/test/simulate_order', methods=['POST'])
def simulate_order():
    if not odoo: return jsonify({"message": "Odoo Offline"}), 500
    
    test_email = os.getenv('ODOO_USERNAME') 
    test_order_ref = f"ONLINE_#SIM{random.randint(100,999)}"
    partner = odoo.search_partner_by_email(test_email)
    
    if not partner:
        return jsonify({"message": f"Test Failed: Email {test_email} not found"}), 400

    if partner.get('parent_id'):
        invoice_id = partner['parent_id'][0]
        shipping_id = partner['id']
        main_id = invoice_id
    else:
        invoice_id = shipping_id = main_id = partner['id']

    log = SyncLog(entity='Test Connection', status='Success', 
                  message=f"Simulation: {test_order_ref} would bill Parent ID {main_id}")
    db.session.add(log)
    db.session.commit()
    
    return jsonify({"message": f"Hierarchy Test Passed! See Dashboard Logs."})

# --- JOB 1: ORDER SYNC (REAL WEBHOOK) ---
@app.route('/webhook/orders', methods=['POST'])
def order_webhook():
    if not odoo: return "Offline", 500
    
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')):
        return "Unauthorized", 401
    
    data = request.json
    email = data.get('email')
    partner = odoo.search_partner_by_email(email)
    
    if not partner:
        log = SyncLog(entity='Order', status='Skipped', message=f"Customer {email} not found in Odoo")
        db.session.add(log)
        db.session.commit()
        return "Skipped", 200

    # Parent/Child Resolution Logic
    if partner.get('parent_id'):
        invoice_id = partner['parent_id'][0] 
        shipping_id = partner['id']          
        main_id = invoice_id
    else:
        invoice_id = shipping_id = main_id = partner['id']

    lines = []
    for item in data.get('line_items', []):
        sku = item.get('sku')
        if not sku: continue
        product_id = odoo.search_product_by_sku(sku)
        
        if product_id:
            price = float(item.get('price', 0))
            qty = int(item.get('quantity', 1))
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

    # Process Shipping Lines
    shipping_lines = data.get('shipping_lines', [])
    if shipping_lines:
        for ship in shipping_lines:
            cost = float(ship.get('price', 0))
            title = ship.get('title', 'Shipping')
            
            shipping_product_id = odoo.search_product_by_name(title)
            
            if not shipping_product_id:
                print(f"DEBUG: Specific shipping '{title}' not found. Trying generic 'Shipping'.")
                shipping_product_id = odoo.search_product_by_name("Shipping")
            
            if cost >= 0 and shipping_product_id:
                lines.append((0, 0, {
                    'product_id': shipping_product_id,
                    'product_uom_qty': 1,
                    'price_unit': cost,
                    'name': title,
                    'is_delivery': True
                }))

    if lines:
        shopify_name = data.get('name') 
        client_ref = f"ONLINE_{shopify_name}" 

        try:
            odoo.create_sale_order({
                'name': client_ref,             
                'client_order_ref': client_ref, 
                'partner_id': main_id,
                'partner_invoice_id': invoice_id,
                'partner_shipping_id': shipping_id,
                'order_line': lines,
                'user_id': odoo.uid, 
                'state': 'draft'     
            })
            
            log = SyncLog(entity='Order', status='Success', message=f"Order {client_ref} synced. Ship ID: {shipping_id}")
            db.session.add(log)
            db.session.commit()
        except Exception as e:
            log = SyncLog(entity='Order', status='Error', message=str(e))
            db.session.add(log)
            db.session.commit()
            return f"Error: {str(e)}", 500

    return "Synced", 200

# --- JOB 1.5: ORDER CANCELLATION SYNC (Shopify -> Odoo) ---
@app.route('/webhook/orders/cancelled', methods=['POST'])
def order_cancelled_webhook():
    if not odoo: return "Offline", 500
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')):
        return "Unauthorized", 401

    data = request.json
    shopify_name = data.get('name')
    client_ref = f"ONLINE_{shopify_name}"

    # Search for this order in Odoo
    # We look for orders with this reference that are NOT already cancelled
    order_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
        'sale.order', 'search', [[['client_order_ref', '=', client_ref], ['state', '!=', 'cancel']]])

    if order_ids:
        try:
            odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'sale.order', 'action_cancel', [order_ids])
            
            log = SyncLog(entity='Order Cancel', status='Success', message=f"Cancelled Odoo Order {client_ref}")
            db.session.add(log)
            db.session.commit()
            return "Cancelled", 200
        except Exception as e:
            log = SyncLog(entity='Order Cancel', status='Error', message=str(e))
            db.session.add(log)
            db.session.commit()
            return f"Error: {str(e)}", 500
            
    return "Order not found or already cancelled", 200

# --- JOB 2: INVENTORY SYNC ---
@app.route('/sync/inventory', methods=['GET'])
def sync_inventory():
    if not odoo: return jsonify({"error": "Offline"}), 500
    
    last_run = datetime.utcnow() - timedelta(minutes=35)
    try:
        product_ids = odoo.get_changed_products(str(last_run))
    except:
        return jsonify({"error": "Read Failed"}), 500
    
    updated_count = 0
    for p_id in product_ids:
        total_qty = odoo.get_total_qty_for_locations(p_id, ODOO_LOCATION_IDS)
        updated_count += 1
        
        if updated_count == 1:
             p_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'read', [p_id], {'fields': ['default_code']})
             sku = p_data[0].get('default_code')
             log = SyncLog(entity='Inventory', status='Info', message=f"Synced SKU {sku}: Qty {total_qty}")
             db.session.add(log)
             db.session.commit()

    return jsonify({"synced": updated_count})

# --- JOB 3: ORDER STATUS SYNC (Odoo -> Shopify) ---
@app.route('/sync/order_status', methods=['GET'])
def sync_order_status():
    if not odoo: return jsonify({"error": "Offline"}), 500
    
    # 1. Find orders cancelled in Odoo recently (last 35 mins)
    last_run = datetime.utcnow() - timedelta(minutes=35)
    domain = [('write_date', '>', str(last_run)), ('state', '=', 'cancel')]
    
    cancelled_orders = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
        'sale.order', 'search_read', [domain], {'fields': ['client_order_ref']})
    
    updated_count = 0
    headers = {
        "X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN'),
        "Content-Type": "application/json"
    }
    
    for order in cancelled_orders:
        ref = order.get('client_order_ref', '')
        # Only process orders created by this app (ONLINE_#...)
        if ref and ref.startswith('ONLINE_#'):
            shopify_name = ref.replace('ONLINE_', '') # e.g. #2046
            
            # 2. Find Shopify ID using the Name
            search_url = f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders.json?name={shopify_name}&status=open"
            res = requests.get(search_url, headers=headers)
            
            if res.status_code == 200 and res.json().get('orders'):
                shopify_order_id = res.json()['orders'][0]['id']
                
                # 3. Cancel in Shopify
                cancel_url = f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders/{shopify_order_id}/cancel.json"
                requests.post(cancel_url, headers=headers)
                
                log = SyncLog(entity='Status Sync', status='Success', message=f"Cancelled Shopify Order {shopify_name}")
                db.session.add(log)
                db.session.commit()
                updated_count += 1

    return jsonify({"cancelled_syncs": updated_count})

if __name__ == '__main__':
    app.run(debug=True)
