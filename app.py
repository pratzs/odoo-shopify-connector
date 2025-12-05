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
# Database connection string from Render/Supabase
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# 1. Parse Multiple Odoo Locations
# Example Env Var: "12,15,22" -> [12, 15, 22]
location_env = os.getenv('ODOO_STOCK_LOCATION_IDS', '0')
ODOO_LOCATION_IDS = [int(x) for x in location_env.split(',') if x.strip().isdigit()]

SHOPIFY_LOCATION_ID = int(os.getenv('SHOPIFY_WAREHOUSE_ID', '0'))

db.init_app(app)

# Initialize Odoo Connection
odoo = OdooClient(
    url=os.getenv('ODOO_URL'),
    db=os.getenv('ODOO_DB'),
    username=os.getenv('ODOO_USERNAME'),
    password=os.getenv('ODOO_PASSWORD')
)

# Create Database Tables (if not exist)
with app.app_context():
    db.create_all()

def verify_shopify(data, hmac_header):
    """Verifies that the webhook request actually came from Shopify"""
    secret = os.getenv('SHOPIFY_SECRET')
    if not secret: return True # Bypass if no secret is set
    digest = hmac.new(secret.encode('utf-8'), data, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), hmac_header)

@app.route('/')
def home():
    return f"Connector Online. Syncing Odoo Locations: {ODOO_LOCATION_IDS} | Target API: 2025-10"

# --- JOB 1: ORDER SYNC (Shopify -> Odoo) ---
@app.route('/webhook/orders', methods=['POST'])
def order_webhook():
    # 1. Verify Webhook
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')):
        return "Unauthorized", 401
    
    data = request.json
    email = data.get('email')
    
    # 2. Find Customer in Odoo
    partner = odoo.search_partner_by_email(email)
    
    if not partner:
        print(f"Skipping: Customer {email} not found in Odoo")
        return "Skipped", 200

    # 3. B2B Hierarchy Logic
    # If customer has a Parent ID, that is the "Company".
    # We bill the Company (Parent) but ship to the Manager (Child).
    if partner.get('parent_id'):
        invoice_id = partner['parent_id'][0] 
        shipping_id = partner['id']
        main_id = invoice_id
    else:
        invoice_id = shipping_id = main_id = partner['id']

    # 4. Map Order Lines
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

    # 5. Create Order
    if lines:
        try:
            odoo.create_sale_order({
                'partner_id': main_id,
                'partner_invoice_id': invoice_id,
                'partner_shipping_id': shipping_id,
                'client_order_ref': data.get('name'),
                'order_line': lines
            })
            
            # Log Success
            log = SyncLog(entity='Order', status='Success', message=f"Order {data.get('name')} synced")
            db.session.add(log)
            db.session.commit()
        except Exception as e:
            return f"Error: {str(e)}", 500

    return "Synced", 200

# --- JOB 2: INVENTORY SYNC (Odoo -> Shopify) ---
@app.route('/sync/inventory', methods=['GET'])
def sync_inventory():
    # 1. Look for products changed in last 35 minutes
    last_run = datetime.utcnow() - timedelta(minutes=35)
    product_ids = odoo.get_changed_products(str(last_run))
    
    updated_count = 0
    
    # 2. Config for Shopify API 2025-10
    shopify_base_url = f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10"
    headers = {
        "X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN'),
        "Content-Type": "application/json"
    }

    for p_id in product_ids:
        # 3. Sum stock from ALL configured Odoo locations (Loose + Bulk)
        total_qty = odoo.get_total_qty_for_locations(p_id, ODOO_LOCATION_IDS)
        
        # Get SKU for logging
        p_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'product.product', 'read', [p_id], {'fields': ['default_code']})
        
        sku = p_data[0].get('default_code')
        
        if sku:
            # To Enable Writes: Uncomment the requests.post lines below
            # You would need to fetch the InventoryItemID from Shopify first
            print(f"SYNC [2025-10]: SKU {sku} Total: {total_qty} -> Shopify Loc {SHOPIFY_LOCATION_ID}")
            updated_count += 1

    return jsonify({"synced": updated_count, "message": "Multi-Location Scan Complete (2025-10)"})

if __name__ == '__main__':
    app.run(debug=True)
