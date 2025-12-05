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
# These values come from Render Environment Variables
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Techmarbles Replacement Settings (IDs you will set in Render)
ODOO_LOCATION_ID = int(os.getenv('ODOO_STOCK_LOCATION_ID', '0')) # The ID for VJW01/Stock/LOOSE
SHOPIFY_LOCATION_ID = int(os.getenv('SHOPIFY_WAREHOUSE_ID', '0')) # The ID for VJ Trading Warehouse

db.init_app(app)

# Initialize Odoo Connection
odoo = OdooClient(
    url=os.getenv('ODOO_URL'),
    db=os.getenv('ODOO_DB'),
    username=os.getenv('ODOO_USERNAME'),
    password=os.getenv('ODOO_PASSWORD')
)

# Create Database Tables if they don't exist
with app.app_context():
    db.create_all()

def verify_shopify(data, hmac_header):
    secret = os.getenv('SHOPIFY_SECRET')
    if not secret: return True 
    digest = hmac.new(secret.encode('utf-8'), data, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), hmac_header)

@app.route('/')
def home():
    return "Custom Odoo-Shopify Connector is Online."

# --- JOB 1: ORDER SYNC (Shopify -> Odoo) ---
# Matches 'image_2d1ee2.png' settings
@app.route('/webhook/orders', methods=['POST'])
def order_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')):
        return "Unauthorized", 401
    
    data = request.json
    email = data.get('email')
    
    # 1. B2B Logic (Your core requirement)
    partner = odoo.search_partner_by_email(email)
    
    # Default to creating a new customer if not found (Optional logic)
    if not partner:
        print(f"Customer {email} not found. Needs creation logic.")
        return "Skipped", 200

    # 2. Parent/Child Resolution
    if partner.get('parent_id'):
        invoice_id = partner['parent_id'][0] # Bill the Owner (Parent)
        shipping_id = partner['id']          # Ship to the Store (Child)
        main_id = invoice_id
    else:
        invoice_id = shipping_id = main_id = partner['id']

    # 3. Map Order Lines
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
                'name': item['name'] # Keeps the name from Shopify
            }))

    # 4. Create Sale Order in Odoo
    if lines:
        try:
            odoo.create_sale_order({
                'partner_id': main_id,
                'partner_invoice_id': invoice_id,
                'partner_shipping_id': shipping_id,
                'client_order_ref': data.get('name'), # Matches Techmarbles 'Set Odoo order number same as Shopify'
                'order_line': lines
            })
            
            # Log it
            log = SyncLog(entity='Order', status='Success', message=f"Order {data.get('name')} synced to Partner {main_id}")
            db.session.add(log)
            db.session.commit()
            return "Synced", 200
        except Exception as e:
            return f"Error: {str(e)}", 500

    return "Synced", 200

# --- JOB 2: INVENTORY SYNC (Odoo -> Shopify) ---
# Matches 'image_2d1f1e.png' logic
@app.route('/sync/inventory', methods=['GET'])
def sync_inventory():
    # Techmarbles logic: Sync every 30 mins.
    # We fetch products changed in the last 35 mins to be safe.
    last_run = datetime.utcnow() - timedelta(minutes=35)
    products = odoo.get_changed_products(str(last_run), location_id=ODOO_LOCATION_ID)
    
    updated_count = 0
    # In a production app, you would POST this data to Shopify.
    # For now, we print it to the logs so you can verify it works before enabling the writes.
    
    for p in products:
        sku = p['default_code']
        # This gets stock specifically from 'VJW01/Stock/LOOSE'
        qty = p['qty_available'] 
        
        if sku:
            print(f"SYNC: Odoo SKU {sku} has {qty} units in Location {ODOO_LOCATION_ID}. Target Shopify Location: {SHOPIFY_LOCATION_ID}")
            updated_count += 1

    return jsonify({"synced": updated_count, "message": "Inventory Check Complete"})

if __name__ == '__main__':
    app.run(debug=True)
