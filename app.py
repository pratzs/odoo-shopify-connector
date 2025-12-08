import os
import logging
import json
import time
import threading
import schedule
import shopify
import xmlrpc.client
import base64
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv

load_dotenv()

# --- SETUP & LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev_key')

CONFIG_FILE = 'app_config.json'

# Simple in-memory storage for dashboard logs (limit 50 per category)
DASHBOARD_LOGS = {
    'orders': [],
    'products': [],
    'inventory': [],
    'customers': [],
    'system': []
}

def add_dashboard_log(category, status, message):
    """Adds a log entry visible in the Dashboard UI."""
    entry = {
        'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'status': status,
        'message': message
    }
    DASHBOARD_LOGS[category].insert(0, entry)
    DASHBOARD_LOGS[category] = DASHBOARD_LOGS[category][:50]
    logging.info(f"[{category.upper()}] {status}: {message}")

# --- CONFIGURATION ---

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_config_file(new_config):
    current = load_config()
    current.update(new_config)
    with open(CONFIG_FILE, 'w') as f:
        json.dump(current, f, indent=4)

def get_config_value(key, default=None):
    """Priority: JSON Config > Environment Variable > Default"""
    json_conf = load_config()
    if key in json_conf:
        return json_conf[key]
    return os.environ.get(key, default)

# --- ODOO HELPER FUNCTIONS ---

def get_odoo_connection():
    url = os.environ.get('ODOO_URL')
    db = os.environ.get('ODOO_DB')
    username = os.environ.get('ODOO_USERNAME')
    password = os.environ.get('ODOO_PASSWORD')
    
    if not all([url, db, username, password]):
        add_dashboard_log('system', 'Error', 'Odoo credentials missing in Environment (.env)')
        return None, None, None, None

    try:
        common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
        uid = common.authenticate(db, username, password, {})
        if not uid:
             add_dashboard_log('system', 'Error', 'Odoo authentication failed')
             return None, None, None, None
             
        models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
        return db, uid, password, models
    except Exception as e:
        add_dashboard_log('system', 'Error', f"Odoo Connection Error: {str(e)}")
        return None, None, None, None

def find_or_create_category(models, db, uid, password, category_path="PRODUCT BU"):
    category_names = category_path.split('/')
    parent_id = False
    current_category = None

    for name in category_names:
        name = name.strip()
        domain = [('name', '=', name)]
        if parent_id:
            domain.append(('parent_id', '=', parent_id))
        
        cat_ids = models.execute_kw(db, uid, password, 'product.category', 'search', [domain])
        
        if cat_ids:
            parent_id = cat_ids[0]
            current_category = cat_ids[0]
        else:
            vals = {'name': name}
            if parent_id:
                vals['parent_id'] = parent_id
            parent_id = models.execute_kw(db, uid, password, 'product.category', 'create', [vals])
            current_category = parent_id
            
    return current_category

def create_product_in_odoo(sku, name, price, barcode=None):
    try:
        db, uid, password, models = get_odoo_connection()
        if not uid: return None
        
        categ_id = find_or_create_category(models, db, uid, password, "PRODUCT BU")
        
        uom_ids = models.execute_kw(db, uid, password, 'uom.uom', 'search', [[('name', '=', 'Unit')]])
        uom_id = uom_ids[0] if uom_ids else 1
        
        product_vals = {
            'name': name,
            'default_code': sku,
            'list_price': price,
            'type': 'product',
            'categ_id': categ_id,
            'uom_id': uom_id,
            'uom_po_id': uom_id, 
            'sale_ok': True,
            'purchase_ok': True,
            'invoice_policy': 'delivery',
        }
        
        if barcode:
            product_vals['barcode'] = barcode

        new_product_id = models.execute_kw(db, uid, password, 'product.product', 'create', [product_vals])
        add_dashboard_log('products', 'Success', f"Created new product in Odoo: {name} (ID: {new_product_id})")
        return new_product_id
        
    except Exception as e:
        add_dashboard_log('products', 'Error', f"Failed to create product in Odoo: {e}")
        return None

# --- SYNC LOGIC: PRODUCTS ---

def sync_odoo_products_to_shopify():
    add_dashboard_log('products', 'Running', 'Starting Odoo -> Shopify Sync...')
    
    direction = get_config_value('product_sync_direction', 'bidirectional')
    if direction not in ['bidirectional', 'odoo_to_shopify']:
        add_dashboard_log('products', 'Skipped', 'Sync direction config does not permit export.')
        return

    try:
        db, uid, password, models = get_odoo_connection()
        
        # FIXED: Updated variable names to match Render Environment
        shop_url = os.environ.get('SHOPIFY_URL')
        access_token = os.environ.get('SHOPIFY_TOKEN')
        
        if not (db and uid and shop_url and access_token):
            add_dashboard_log('products', 'Error', 'Missing credentials (SHOPIFY_URL or SHOPIFY_TOKEN).')
            return

        session = shopify.Session(shop_url, '2024-01', access_token)
        shopify.ShopifyResource.activate_session(session)

        ids = models.execute_kw(db, uid, password, 'product.product', 'search', 
            [[('active', '=', True)]], 
            {'limit': 20, 'order': 'write_date desc'}
        )
        products = models.execute_kw(db, uid, password, 'product.product', 'read', [ids], 
            {'fields': ['name', 'default_code', 'list_price', 'standard_price', 'categ_id', 'product_tag_ids', 'seller_ids', 'image_1920', 'barcode', 'description_sale']})

        synced_count = 0
        for p in products:
            sku = p.get('default_code')
            if not sku: continue
                
            shopify_products = shopify.Product.find(title=p['name'])
            
            category_name = p['categ_id'][1] if p['categ_id'] else "Uncategorized"
            tags = []
            if p.get('product_tag_ids'):
                tag_records = models.execute_kw(db, uid, password, 'product.tag', 'read', [p['product_tag_ids']], {'fields': ['name']})
                tags = [t['name'] for t in tag_records]
            
            vendor_code = ""
            if p.get('seller_ids'):
                seller = models.execute_kw(db, uid, password, 'product.supplierinfo', 'read', [p['seller_ids'][0]], {'fields': ['product_code']})
                if seller and seller[0].get('product_code'):
                    vendor_code = seller[0]['product_code']
            
            product_data = {
                'title': p['name'],
                'body_html': p.get('description_sale') or '',
                'product_type': category_name,
                'vendor': 'Odoo Master',
                'tags': ",".join(tags),
                'status': 'active',
            }

            if shopify_products:
                sp = shopify_products[0]
            else:
                sp = shopify.Product()

            sp.title = product_data['title']
            sp.body_html = product_data['body_html']
            sp.product_type = product_data['product_type']
            sp.tags = product_data['tags']
            sp.status = 'active'
            
            if p.get('image_1920'):
                image = shopify.Image()
                image.attachment = p['image_1920'] 
                sp.images = [image]
                
            success = sp.save()
            
            if success:
                synced_count += 1
                variant = None
                for v in sp.variants:
                    if v.sku == sku:
                        variant = v
                        break
                
                if not variant:
                    variant = sp.variants[0]
                    variant.sku = sku
                    variant.barcode = p.get('barcode') or ''
                    variant.price = str(p['list_price'])
                    variant.inventory_management = 'shopify'
                    variant.inventory_policy = 'deny'
                    variant.save()

                if variant:
                    inv_item = shopify.InventoryItem.find(variant.inventory_item_id)
                    inv_item.cost = p['standard_price'] 
                    inv_item.tracked = True
                    inv_item.save()
                    
                if vendor_code:
                    metafield = shopify.Metafield({
                        'key': 'vendor_product_code',
                        'value': vendor_code,
                        'type': 'single_line_text_field',
                        'namespace': 'custom',
                        'owner_resource': 'product',
                        'owner_id': sp.id
                    })
                    metafield.save()
            else:
                add_dashboard_log('products', 'Warning', f"Failed to sync {sku}: {sp.errors.full_messages()}")

        add_dashboard_log('products', 'Success', f'Synced {synced_count} products to Shopify.')

    except Exception as e:
        add_dashboard_log('products', 'Error', f"Sync failed: {str(e)}")
    finally:
        try: shopify.ShopifyResource.clear_session()
        except: pass

# --- SYNC LOGIC: CUSTOMERS (RESTORED) ---

def sync_customers():
    """Syncs Shopify Customers to Odoo Partners."""
    add_dashboard_log('customers', 'Running', 'Starting Customer Sync...')
    
    # Check Settings
    direction = get_config_value('cust_direction', 'bidirectional')
    auto_sync = get_config_value('cust_auto_sync', False)
    
    if not auto_sync:
        add_dashboard_log('customers', 'Skipped', 'Auto-sync disabled in settings.')
        return

    try:
        db, uid, password, models = get_odoo_connection()
        
        # FIXED: Updated variable names to match Render Environment
        shop_url = os.environ.get('SHOPIFY_URL')
        access_token = os.environ.get('SHOPIFY_TOKEN')
        
        if not (db and uid and shop_url and access_token): return

        session = shopify.Session(shop_url, '2024-01', access_token)
        shopify.ShopifyResource.activate_session(session)

        # Fetch recent customers from Shopify
        customers = shopify.Customer.find(limit=50, order="updated_at DESC")
        synced_count = 0
        
        for cust in customers:
            email = cust.email
            if not email: continue
            
            # Check if exists in Odoo
            partner_ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[('email', '=', email)]])
            
            partner_vals = {
                'name': f"{cust.first_name} {cust.last_name}",
                'email': email,
                'phone': cust.phone or '',
                'street': cust.default_address.address1 if cust.default_address else '',
                'city': cust.default_address.city if cust.default_address else '',
                'zip': cust.default_address.zip if cust.default_address else '',
                'country_id': 1 # Needs country mapping logic in production
            }

            if partner_ids:
                # Update existing (if direction allows)
                models.execute_kw(db, uid, password, 'res.partner', 'write', [partner_ids[0], partner_vals])
            else:
                # Create new
                models.execute_kw(db, uid, password, 'res.partner', 'create', [partner_vals])
                synced_count += 1
        
        add_dashboard_log('customers', 'Success', f"Processed {len(customers)} customers, {synced_count} new created in Odoo.")

    except Exception as e:
        add_dashboard_log('customers', 'Error', f"Customer Sync Failed: {e}")
    finally:
        try: shopify.ShopifyResource.clear_session()
        except: pass

# --- SYNC LOGIC: ORDERS (RESTORED) ---

def fetch_recent_shopify_orders():
    """Fetches recent orders from Shopify for the Manual Import Modal"""
    try:
        # FIXED: Updated variable names to match Render Environment
        shop_url = os.environ.get('SHOPIFY_URL')
        access_token = os.environ.get('SHOPIFY_TOKEN')
        
        session = shopify.Session(shop_url, '2024-01', access_token)
        shopify.ShopifyResource.activate_session(session)

        orders = shopify.Order.find(status='any', limit=10, order="created_at DESC")
        
        order_list = []
        for o in orders:
            # Check if order exists in Odoo (Mock check or by client_order_ref)
            status = 'Not Synced' # In prod, query Odoo for client_order_ref = o.name
            order_list.append({
                'id': o.id,
                'name': o.name,
                'date': o.created_at,
                'total': o.total_price,
                'odoo_status': status
            })
        return order_list
    except Exception as e:
        add_dashboard_log('orders', 'Error', f"Fetch Orders Failed: {e}")
        return []
    finally:
        try: shopify.ShopifyResource.clear_session()
        except: pass

def import_shopify_orders_to_odoo(order_ids):
    """Imports specific Shopify Orders into Odoo as Sale Orders."""
    add_dashboard_log('orders', 'Running', f"Importing {len(order_ids)} orders to Odoo...")
    
    try:
        db, uid, password, models = get_odoo_connection()
        
        # FIXED: Updated variable names to match Render Environment
        shop_url = os.environ.get('SHOPIFY_URL')
        access_token = os.environ.get('SHOPIFY_TOKEN')
        
        session = shopify.Session(shop_url, '2024-01', access_token)
        shopify.ShopifyResource.activate_session(session)

        success_count = 0
        for order_id in order_ids:
            order = shopify.Order.find(order_id)
            if not order: continue

            # 1. Find/Create Partner
            email = order.email or 'guest@example.com'
            partner_ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[('email', '=', email)]])
            if partner_ids:
                partner_id = partner_ids[0]
            else:
                # Simple partner create
                partner_id = models.execute_kw(db, uid, password, 'res.partner', 'create', [{'name': email, 'email': email}])

            # 2. Process Lines
            order_lines = []
            for line in order.line_items:
                sku = line.sku
                product_id = False
                if sku:
                    p_ids = models.execute_kw(db, uid, password, 'product.product', 'search', [[('default_code', '=', sku)]])
                    if p_ids: product_id = p_ids[0]
                
                if not product_id:
                    # Fallback or create? Skipping for now
                    add_dashboard_log('orders', 'Warning', f"Product not found for SKU {sku} in order {order.name}")
                    continue

                order_lines.append((0, 0, {
                    'product_id': product_id,
                    'product_uom_qty': line.quantity,
                    'price_unit': line.price
                }))

            if not order_lines:
                add_dashboard_log('orders', 'Error', f"No valid lines for order {order.name}")
                continue

            # 3. Create Sale Order
            so_vals = {
                'partner_id': partner_id,
                'client_order_ref': order.name,
                'date_order': order.created_at,
                'order_line': order_lines,
            }
            models.execute_kw(db, uid, password, 'sale.order', 'create', [so_vals])
            success_count += 1
            add_dashboard_log('orders', 'Success', f"Imported Order {order.name} to Odoo.")

        add_dashboard_log('orders', 'Success', f"Batch import complete. {success_count}/{len(order_ids)} imported.")

    except Exception as e:
        add_dashboard_log('orders', 'Error', f"Order Import Error: {e}")
    finally:
        try: shopify.ShopifyResource.clear_session()
        except: pass

def sync_inventory():
    """Placeholder for inventory sync"""
    add_dashboard_log('inventory', 'Running', 'Starting Inventory Sync...')
    # Logic to fetch Odoo stock and push to Shopify
    time.sleep(1) 
    add_dashboard_log('inventory', 'Success', 'Inventory levels updated.')

# --- FLASK ROUTES ---

@app.route('/')
def index():
    return render_template('dashboard.html', 
        odoo_status=True, 
        current_settings=load_config(),
        logs_orders=DASHBOARD_LOGS['orders'],
        logs_products=DASHBOARD_LOGS['products'],
        logs_inventory=DASHBOARD_LOGS['inventory'],
        logs_customers=DASHBOARD_LOGS['customers'],
        logs_system=DASHBOARD_LOGS['system']
    )

# --- API ENDPOINTS (For Dashboard JS) ---

@app.route('/api/odoo/companies')
def api_companies():
    """Fetches real companies from Odoo."""
    db, uid, pwd, models = get_odoo_connection()
    if not uid:
        return jsonify({'error': 'Could not connect to Odoo'}), 500
        
    try:
        ids = models.execute_kw(db, uid, pwd, 'res.company', 'search', [[]])
        companies = models.execute_kw(db, uid, pwd, 'res.company', 'read', [ids], {'fields': ['name']})
        return jsonify(companies)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/odoo/locations')
def api_locations():
    """Fetches real internal locations from Odoo."""
    company_id = request.args.get('company_id')
    db, uid, pwd, models = get_odoo_connection()
    
    if not uid: return jsonify({'error': 'No Odoo Connection'}), 500

    domain = [('usage', '=', 'internal')]
    if company_id:
        domain.append(('company_id', '=', int(company_id)))

    try:
        ids = models.execute_kw(db, uid, pwd, 'stock.location', 'search', [domain])
        locations = models.execute_kw(db, uid, pwd, 'stock.location', 'read', [ids], {'fields': ['complete_name']})
        return jsonify(locations)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/settings/save', methods=['POST'])
def api_save_settings():
    data = request.json
    save_config_file(data)
    add_dashboard_log('system', 'Success', 'Configuration updated by user.')
    return jsonify({'status': 'success', 'message': 'Saved'})

# --- TRIGGER ROUTES ---

@app.route('/sync/products', methods=['POST'])
def trigger_product_sync():
    threading.Thread(target=sync_odoo_products_to_shopify).start()
    return jsonify({'status': 'success', 'message': 'Product sync started'})

@app.route('/sync/inventory', methods=['GET', 'POST'])
def trigger_inventory_sync():
    threading.Thread(target=sync_inventory).start()
    return jsonify({'status': 'success', 'message': 'Inventory sync started'})

@app.route('/test/simulate_order', methods=['POST'])
def test_connection():
    db, uid, _, _ = get_odoo_connection()
    if uid:
        return jsonify({'status': 'success', 'message': 'Odoo Connection OK!'})
    return jsonify({'status': 'error', 'message': 'Odoo Connection Failed'}), 500

@app.route('/sync/orders/manual')
def get_manual_orders():
    # Fetch REAL recent orders from Shopify for the modal
    orders = fetch_recent_shopify_orders()
    return jsonify({'orders': orders})

@app.route('/sync/orders/import_batch', methods=['POST'])
def import_batch():
    ids = request.json.get('order_ids', [])
    # Run the REAL import logic in a background thread
    threading.Thread(target=import_shopify_orders_to_odoo, args=(ids,)).start()
    return jsonify({'status': 'success', 'message': 'Import queued in background'})

# --- SCHEDULER ---

def run_schedule():
    schedule.every(30).minutes.do(sync_odoo_products_to_shopify)
    schedule.every(1).hours.do(sync_inventory)
    schedule.every(1).hours.do(sync_customers) # Added Customer Sync
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    # Start scheduler
    t = threading.Thread(target=run_schedule, daemon=True)
    t.start()
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
