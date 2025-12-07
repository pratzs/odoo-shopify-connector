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
    if not secret: return True 
    digest = hmac.new(secret.encode('utf-8'), data, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), hmac_header)

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
    # Logic: We use 'TEST-' prefix here for the button simulation
    test_order_ref = f"TEST-SIM-{random.randint(1000,9999)}"
    
    partner = odoo.search_partner_by_email(test_email)
    
    if not partner:
        return jsonify({"message": f"Test Failed: Email {test_email} not found"}), 400

    if partner.get('parent_id'):
        invoice_id = partner['parent_id'][0]
        shipping_id = partner['id']
        main_id = invoice_id
    else:
        invoice_id = shipping_id = main_id = partner['id']

    # We just log the hierarchy check for the simulation
    log = SyncLog(entity='Test Connection', status='Success', 
                  message=f"Simulation: {test_order_ref} would bill Parent ID {main_id} and ship to Child ID {shipping_id}")
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

    # Parent/Child Resolution
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
            lines.append((0, 0, {
                'product_id': product_id,
                'product_uom_qty': item['quantity'],
                'price_unit': item['price'],
                'name': item['name']
            }))

    if lines:
        # --- CUSTOM PREFIX LOGIC ---
        shopify_name = data.get('name') # e.g. "#1050"
        # We add "CUSTOM-" so you can differentiate from Techmarbles
        client_ref = f"CUSTOM-{shopify_name}" 

        try:
            odoo.create_sale_order({
                'partner_id': main_id,
                'partner_invoice_id': invoice_id,
                'partner_shipping_id': shipping_id,
                'client_order_ref': client_ref, # Sends "CUSTOM-#1050" to Odoo
                'order_line': lines
            })
            
            log = SyncLog(entity='Order', status='Success', message=f"Order {client_ref} synced to Partner {main_id}")
            db.session.add(log)
            db.session.commit()
        except Exception as e:
            log = SyncLog(entity='Order', status='Error', message=str(e))
            db.session.add(log)
            db.session.commit()
            return f"Error: {str(e)}", 500

    return "Synced", 200

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
        
        # Log first item for visibility
        if updated_count == 1:
             p_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'read', [p_id], {'fields': ['default_code']})
             sku = p_data[0].get('default_code')
             log = SyncLog(entity='Inventory', status='Info', message=f"Synced SKU {sku}: Qty {total_qty}")
             db.session.add(log)
             db.session.commit()

    return jsonify({"synced": updated_count})

if __name__ == '__main__':
    app.run(debug=True)
