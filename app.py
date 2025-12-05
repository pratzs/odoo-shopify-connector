import os
import hmac
import hashlib
import base64
from flask import Flask, request, jsonify
from models import db, ProductMap, SyncLog
from odoo_client import OdooClient
import requests
from datetime import datetime, timedelta

app = Flask(__name__)

# --- CONFIGURATION ---
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# 1. Parse Multiple Odoo Locations
# It reads "12,15" from Render and turns it into a list: [12, 15]
location_env = os.getenv('ODOO_STOCK_LOCATION_IDS', '0')
ODOO_LOCATION_IDS = [int(x) for x in location_env.split(',') if x.strip().isdigit()]

SHOPIFY_LOCATION_ID = int(os.getenv('SHOPIFY_WAREHOUSE_ID', '0'))

db.init_app(app)

# Initialize Odoo
odoo = OdooClient(
    url=os.getenv('ODOO_URL'),
    db=os.getenv('ODOO_DB'),
    username=os.getenv('ODOO_USERNAME'),
    password=os.getenv('ODOO_PASSWORD')
)

# Create Tables
with app.app_context():
    db.create_all()

def verify_shopify(data, hmac_header):
    secret = os.getenv('SHOPIFY_SECRET')
    if not secret: return True 
    digest = hmac.new(secret.encode('utf-8'), data, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), hmac_header)

@app.route('/')
def home():
    # Shows status and API version on the homepage
    return f"Connector Online. Syncing Odoo Locations: {ODOO_LOCATION_IDS} | Target API: 2025-10"

# --- JOB 1: ORDER SYNC ---
@app.route('/webhook/orders', methods=['POST'])
def order_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')):
        return "Unauthorized", 401
    
    data = request.json
    email = data.get('email')
    
    # B2B Logic
    partner = odoo.search_partner_by_email(email)
    
    if not partner:
        # In a real app, you would create the customer here
        print(f"Skipping order: {email} not found in Odoo")
        return "Skipped", 200

    # Parent/Child Resolution
    if partner.get('parent_id'):
        invoice_id = partner['parent_id'][0] # Bill Parent
        shipping_id = partner['id']          # Ship Store
        main_id = invoice_id
    else:
        invoice_id = shipping_id = main_id = partner['id']

    # Map Lines
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
        try:
            odoo.create_sale_order({
                'partner_id': main_id,
                'partner_invoice_id': invoice_id,
                'partner_shipping_id': shipping_id,
                'client_order_ref': data.get('name'),
                'order_line': lines
            })
            
            # Log
            log = SyncLog(entity='Order', status='Success', message=f"Order {data.get('name')} synced")
            db.session.add(log)
            db.session.commit()
        except Exception as e:
            return f"Error: {str(e)}", 500

    return "Synced", 200

# --- JOB 2: INVENTORY SYNC (API 2025-10) ---
@app.route('/sync/inventory', methods=['GET'])
def sync_inventory():
    last_run = datetime.utcnow() - timedelta(minutes=35)
    
    # 1. Get list of product IDs modified recently
    # This relies on odoo_client.py which returns IDs
    product_ids = odoo.get_changed_products(str(last_run))
    
    updated_count = 0
    
    # Updated API Version here
    shopify_base_url = f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10"
    headers = {
        "X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN'),
        "Content-Type": "application/json"
    }

    for p_id in product_ids:
        # 2. Get TOTAL stock across all defined locations [12, 15, etc.]
        total_qty = odoo.get_total_qty_for_locations(p_id, ODOO_LOCATION_IDS)
        
        # We perform a small read just to get the SKU for the logs
        p_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'product.product', 'read', [p_id], {'fields': ['default_code']})
        
        sku = p_data[0].get('default_code')
        
        if sku:
            # OPTIONAL: To enable writing to Shopify, uncomment below.
            # You would need to fetch the InventoryItemID first using the SKU.
            # requests.post(f"{shopify_base_url}/inventory_levels/set.json", ...)
            
            print(f"SYNC [2025-10]: SKU {sku} Total Stock: {total_qty} -> Shopify Loc {SHOPIFY_LOCATION_ID}")
            updated_count += 1

    return jsonify({"synced": updated_count, "message": "Multi-Location Scan Complete (2025-10)"})

if __name__ == '__main__':
    app.run(debug=True)
