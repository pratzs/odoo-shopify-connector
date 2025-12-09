import os
import hmac
import hashlib
import base64
import json
import threading
import schedule
import time
import shopify 
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

def extract_id(res):
    if isinstance(res, list) and len(res) > 0:
        return res[0]
    return res

def setup_shopify_session():
    """Initializes the Shopify Session"""
    shop_url = os.getenv('SHOPIFY_URL')
    token = os.getenv('SHOPIFY_TOKEN')
    if not shop_url or not token: return False
    session = shopify.Session(shop_url, '2024-01', token)
    shopify.ShopifyResource.activate_session(session)
    return True

# --- GRAPHQL HELPERS ---
def find_shopify_product_by_sku(sku):
    """Finds a Shopify Product ID by SKU using GraphQL to avoid duplicates"""
    query = """
    {
      productVariants(first: 1, query: "sku:%s") {
        edges {
          node {
            product {
              legacyResourceId
            }
          }
        }
      }
    }
    """ % sku
    try:
        client = shopify.GraphQL()
        result = client.execute(query)
        data = json.loads(result)
        edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
        if edges:
            return edges[0]['node']['product']['legacyResourceId']
    except Exception as e:
        print(f"GraphQL Error: {e}")
    return None

# --- CORE LOGIC ---

def process_order_data(data):
    """Syncs order. If SKU missing in Odoo, creates it first."""
    email = data.get('email') or data.get('contact_email')
    shopify_name = data.get('name')
    client_ref = f"ONLINE_{shopify_name}"
    company_id = get_config('odoo_company_id')
    
    if not company_id and odoo:
        try:
            user_info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                'res.users', 'read', [[odoo.uid]], {'fields': ['company_id']})
            if user_info: company_id = user_info[0]['company_id'][0]
        except: pass

    # Check for existing order
    try:
        existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
    except Exception as e: return False, f"Odoo Error: {str(e)}"

    # 1. Customer Resolution
    partner = odoo.search_partner_by_email(email)
    if not partner:
        # Create Customer
        cust_data = data.get('customer', {})
        def_address = data.get('billing_address') or data.get('shipping_address') or {}
        name = f"{cust_data.get('first_name', '')} {cust_data.get('last_name', '')}".strip() or def_address.get('name') or email
        vals = {
            'name': name, 'email': email, 'phone': cust_data.get('phone'),
            'company_type': 'company', 'street': def_address.get('address1'),
            'city': def_address.get('city'), 'zip': def_address.get('zip'), 'country_code': def_address.get('country_code')
        }
        if company_id: vals['company_id'] = int(company_id)
        try:
            partner_id = odoo.create_partner(vals)
            partner = {'id': partner_id, 'name': name, 'parent_id': False}
            log_event('Customer', 'Success', f"Created Customer: {name}")
        except Exception as e:
            return False, f"Customer Error: {e}"
    
    partner_id = extract_id(partner['parent_id'][0] if partner.get('parent_id') else partner['id'])
    
    # Salesperson Lookup
    sales_rep_id = odoo.get_partner_salesperson(partner_id)
    if not sales_rep_id: sales_rep_id = odoo.uid

    # 2. Addresses (Delivery/Invoice) - Logic omitted for brevity (same as before)
    shipping_id = partner_id # Placeholder for full logic
    invoice_id = partner_id  # Placeholder for full logic

    # 3. Build Lines & Create Missing Products
    lines = []
    for item in data.get('line_items', []):
        sku = item.get('sku')
        if not sku: continue

        product_id = odoo.search_product_by_sku(sku, company_id)
        
        # --- NEW LOGIC: Create Product in Odoo if missing ---
        if not product_id:
            log_event('Product', 'Info', f"SKU {sku} missing in Odoo. Creating...")
            try:
                new_p_vals = {
                    'name': item['name'],
                    'default_code': sku,
                    'list_price': float(item.get('price', 0)),
                    'type': 'product'
                }
                if company_id: new_p_vals['company_id'] = int(company_id)
                odoo.create_product(new_p_vals)
                product_id = odoo.search_product_by_sku(sku, company_id) # Search again
            except Exception as e:
                log_event('Product', 'Error', f"Failed to create SKU {sku}: {e}")

        if product_id:
            price = float(item.get('price', 0))
            qty = int(item.get('quantity', 1))
            lines.append((0, 0, {'product_id': product_id, 'product_uom_qty': qty, 'price_unit': price, 'name': item['name']}))
        else:
            log_event('Order', 'Warning', f"Skipped line {sku}: Could not create/find product.")

    # Shipping lines logic (same as before)
    # ...

    if not lines: return False, "No valid lines"
    
    # 4. Sync Order
    vals = {
        'name': client_ref, 'client_order_ref': client_ref,
        'partner_id': partner_id, 'partner_invoice_id': invoice_id, 'partner_shipping_id': shipping_id,
        'order_line': lines, 
        'user_id': sales_rep_id,
        'state': 'draft'
    }
    if company_id: vals['company_id'] = int(company_id)
    
    try:
        if existing_ids:
            # Update logic
            return True, "Updated"
        else:
            odoo.create_sale_order(vals)
            log_event('Order', 'Success', f"Synced {client_ref}")
            return True, "Synced"
    except Exception as e:
        log_event('Order', 'Error', str(e))
        return False, str(e)

def sync_products_master():
    """Odoo is Master: Pushes all Odoo products to Shopify"""
    if not odoo or not setup_shopify_session(): 
        log_event('System', 'Error', "Sync Failed: Connection Error")
        return

    company_id = get_config('odoo_company_id')
    odoo_products = odoo.get_all_products(company_id)
    
    log_event('Product Sync', 'Info', f"Found {len(odoo_products)} products in Odoo. Starting Sync...")
    
    synced = 0
    for p in odoo_products:
        sku = p.get('default_code')
        if not sku: continue

        # Check Shopify by SKU (Unique ID)
        shopify_id = find_shopify_product_by_sku(sku)
        
        try:
            if shopify_id:
                sp = shopify.Product.find(shopify_id)
            else:
                sp = shopify.Product()
            
            # Map Data (Odoo -> Shopify)
            sp.title = p['name']
            sp.body_html = p.get('description_sale') or ''
            sp.product_type = 'Storable Product'
            sp.vendor = 'Odoo Master'
            sp.status = 'active'
            
            # Save parent first to get ID
            sp.save()
            
            # Handle Variants (Simplified: 1 variant per product for now)
            if sp.variants:
                variant = sp.variants[0]
            else:
                variant = shopify.Variant()
                
            variant.sku = sku
            variant.price = str(p['list_price'])
            variant.barcode = p.get('barcode') or ''
            variant.weight = p.get('weight', 0)
            variant.inventory_management = 'shopify'
            
            # Attach to product
            variant.product_id = sp.id
            variant.save()
            
            synced += 1
        except Exception as e:
            log_event('Product Sync', 'Error', f"Failed {sku}: {e}")
            
    log_event('Product Sync', 'Success', f"Master Sync Complete. Processed {synced} products.")

def archive_shopify_duplicates():
    """Scans Shopify for duplicate SKUs and archives the older ones."""
    if not setup_shopify_session(): return
    
    log_event('Duplicate Scan', 'Info', "Starting Duplicate SKU Scan...")
    
    # 1. Fetch all variants (Pagination required in prod, simplified here)
    # Using GraphQL is best for this, but REST is easier to code quickly for "Scan"
    # We will fetch variants and group by SKU
    sku_map = {}
    
    # Fetching page 1 (Limit 250). In production, loop pages.
    variants = shopify.Variant.find(limit=250) 
    
    count = 0
    for v in variants:
        if not v.sku: continue
        if v.sku not in sku_map:
            sku_map[v.sku] = []
        sku_map[v.sku].append(v)

    # 2. Check for duplicates
    for sku, var_list in sku_map.items():
        if len(var_list) > 1:
            # Sort by ID (descending) -> assuming higher ID is newer
            var_list.sort(key=lambda x: x.id, reverse=True)
            
            # Keep the first one (newest), archive the rest
            keep = var_list[0]
            duplicates = var_list[1:]
            
            for dup in duplicates:
                try:
                    prod = shopify.Product.find(dup.product_id)
                    prod.status = 'archived'
                    prod.save()
                    count += 1
                    log_event('Duplicate Scan', 'Warning', f"Archived duplicate product {prod.title} (SKU: {sku})")
                except Exception as e:
                    print(f"Archive fail: {e}")

    if count == 0:
        log_event('Duplicate Scan', 'Success', "Clean! No duplicates found.")
    else:
        log_event('Duplicate Scan', 'Success', f"Archived {count} duplicate products.")

# --- ROUTES ---

@app.route('/')
def dashboard():
    # ... (Same as before, simplified for brevity) ...
    # Ensure logs_orders, etc. are passed
    try:
        logs_orders = SyncLog.query.filter(SyncLog.entity.in_(['Order', 'Order Cancel'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_inventory = SyncLog.query.filter_by(entity='Inventory').order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_products = SyncLog.query.filter(SyncLog.entity.in_(['Product', 'Product Sync', 'Duplicate Scan'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_customers = SyncLog.query.filter_by(entity='Customer').order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_system = SyncLog.query.filter(SyncLog.entity.notin_(['Order', 'Order Cancel', 'Inventory', 'Customer', 'Product', 'Product Sync', 'Duplicate Scan'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
    except:
        logs_orders = logs_inventory = logs_products = logs_customers = logs_system = []
    
    current_settings = {
        "odoo_company_id": get_config('odoo_company_id', None),
        # ... other settings
    }
    odoo_status = True if odoo else False
    return render_template('dashboard.html', 
                           logs_orders=logs_orders, logs_inventory=logs_inventory, logs_products=logs_products,
                           logs_customers=logs_customers, logs_system=logs_system,
                           odoo_status=odoo_status, current_settings=current_settings)

@app.route('/sync/products/master', methods=['POST'])
def trigger_master_sync():
    threading.Thread(target=sync_products_master).start()
    return jsonify({"message": "Master Product Sync Started (Odoo -> Shopify)"})

@app.route('/sync/products/archive_duplicates', methods=['POST'])
def trigger_duplicate_scan():
    threading.Thread(target=archive_shopify_duplicates).start()
    return jsonify({"message": "Duplicate Scan Started"})

# ... (Previous API/Webhook routes remain the same) ...
# @app.route('/api/odoo/companies')...
# @app.route('/sync/inventory')...
# @app.route('/webhook/orders')...

def run_schedule():
    # Monthly Duplicate Scan (approx 30 days)
    schedule.every(30).days.do(archive_shopify_duplicates)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    t = threading.Thread(target=run_schedule, daemon=True)
    t.start()
    app.run(debug=True)
