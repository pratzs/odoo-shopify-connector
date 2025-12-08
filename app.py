import os
import logging
import shopify
import xmlrpc.client
import time
import schedule
import threading
import json
import base64
from flask import Flask, request, render_template_string, redirect, url_for, flash
from dotenv import load_dotenv

# Load environment variables from .env file (for local dev)
load_dotenv()

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration & Helpers ---

def get_config(key, default=None):
    """
    Get configuration from environment variables.
    In production, you might want to fetch this from a database if configs change dynamically.
    """
    return os.environ.get(key, default)

# --- Odoo Helpers ---

def get_odoo_connection():
    url = get_config('ODOO_URL')
    db = get_config('ODOO_DB')
    username = get_config('ODOO_USERNAME')
    password = get_config('ODOO_PASSWORD')
    
    if not all([url, db, username, password]):
        logging.error("Missing Odoo credentials in environment variables.")
        return None, None, None, None

    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(db, username, password, {})
    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    
    return db, uid, password, models

def find_or_create_category(models, db, uid, password, category_path="PRODUCT BU"):
    """
    Finds a category by name, or creates it if it doesn't exist.
    Handles hierarchy if path is slashed, e.g. "PRODUCT BU/Confectionery"
    """
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
            # Create if not exists
            vals = {'name': name}
            if parent_id:
                vals['parent_id'] = parent_id
            parent_id = models.execute_kw(db, uid, password, 'product.category', 'create', [vals])
            current_category = parent_id
            
    return current_category

def create_product_in_odoo(sku, name, price, barcode=None):
    """
    Creates a product in Odoo with specific defaults.
    """
    try:
        db, uid, password, models = get_odoo_connection()
        if not uid: return None
        
        # 1. Find/Create Category "PRODUCT BU"
        categ_id = find_or_create_category(models, db, uid, password, "PRODUCT BU")
        
        # 2. Find UoM "Unit" (standard usually ID 1, but best to search)
        uom_ids = models.execute_kw(db, uid, password, 'uom.uom', 'search', [[('name', '=', 'Unit')]])
        uom_id = uom_ids[0] if uom_ids else 1
        
        # 3. Prepare Product Values
        product_vals = {
            'name': name,
            'default_code': sku,
            'list_price': price,
            'type': 'product', # Storable Product
            'categ_id': categ_id,
            'uom_id': uom_id,
            'uom_po_id': uom_id, # Purchase UoM
            'sale_ok': True,
            'purchase_ok': True,
            'invoice_policy': 'delivery',
        }
        
        if barcode:
            product_vals['barcode'] = barcode

        new_product_id = models.execute_kw(db, uid, password, 'product.product', 'create', [product_vals])
        logging.info(f"Created new product in Odoo: {name} (ID: {new_product_id})")
        return new_product_id
        
    except Exception as e:
        logging.error(f"Failed to create product in Odoo: {e}")
        return None

# --- Sync Logic: Odoo to Shopify ---

def sync_odoo_products_to_shopify():
    """
    Scheduled job: Syncs new/updated products from Odoo to Shopify.
    Maps: Fields, Tags, Metafields (Vendor Code), Images, Cost.
    """
    logging.info("Starting Odoo -> Shopify Product Sync...")
    
    direction = get_config('product_sync_direction', 'bidirectional')
    if direction not in ['bidirectional', 'odoo_to_shopify']:
        logging.info("Skipping Odoo -> Shopify sync due to configuration.")
        return

    try:
        db, uid, password, models = get_odoo_connection()
        shop_url = get_config('SHOPIFY_SHOP_URL')
        access_token = get_config('SHOPIFY_ACCESS_TOKEN')
        
        if not (shop_url and access_token and uid):
            logging.error("Shopify or Odoo credentials missing.")
            return

        session = shopify.Session(shop_url, '2024-01', access_token)
        shopify.ShopifyResource.activate_session(session)

        # 1. Find modified products in Odoo
        # For this example, we fetch a limited set of active products sorted by write_date
        ids = models.execute_kw(db, uid, password, 'product.product', 'search', 
            [[('active', '=', True)]], 
            {'limit': 20, 'order': 'write_date desc'}
        )
        products = models.execute_kw(db, uid, password, 'product.product', 'read', [ids], 
            {'fields': ['name', 'default_code', 'list_price', 'standard_price', 'categ_id', 'product_tag_ids', 'seller_ids', 'image_1920', 'barcode', 'qty_available', 'description_sale']}
        )

        for p in products:
            sku = p.get('default_code')
            if not sku:
                continue # Cannot sync without SKU
                
            # --- Search logic ---
            # Ideally use GraphQL for SKU search. Here we use a title search approximation for the demo.
            shopify_products = shopify.Product.find(title=p['name'])
            
            # --- Mapping Logic ---
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
                'variants': [{
                    'price': str(p['list_price']),
                    'sku': sku,
                    'barcode': p.get('barcode') or '',
                    'inventory_management': 'shopify',
                    'inventory_policy': 'deny',
                }]
            }

            # --- Create or Update ---
            # Simplified logic: If we found products by name, we assume update, else create.
            if shopify_products:
                sp = shopify_products[0]
            else:
                sp = shopify.Product()

            sp.title = product_data['title']
            sp.body_html = product_data['body_html']
            sp.product_type = product_data['product_type']
            sp.tags = product_data['tags']
            sp.status = 'active'
            
            # Only update image if one exists in Odoo
            if p.get('image_1920'):
                image = shopify.Image()
                image.attachment = p['image_1920'] # Base64 string
                sp.images = [image]
                
            success = sp.save()
            
            if success:
                logging.info(f"Synced product {sku} to Shopify.")
                
                # Post-Save: Update Inventory Item for Cost & Tracked Inventory
                if sp.variants:
                    variant = sp.variants[0]
                    inventory_item_id = variant.inventory_item_id
                    
                    inv_item = shopify.InventoryItem.find(inventory_item_id)
                    inv_item.cost = p['standard_price'] # Odoo Cost Price
                    inv_item.tracked = True
                    inv_item.save()
                    
                    # Handle Metafield (Vendor Code)
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
                logging.error(f"Failed to sync {sku}: {sp.errors.full_messages()}")

    except Exception as e:
        logging.error(f"Error in sync_odoo_products_to_shopify: {e}")
    finally:
        shopify.ShopifyResource.clear_session()

# --- Main App ---

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'supersecretkey_dev_only')

@app.route('/manual_sync_products', methods=['POST'])
def manual_sync_products():
    """Trigger manual sync from Odoo to Shopify"""
    try:
        # Run in thread to not block the response
        t = threading.Thread(target=sync_odoo_products_to_shopify)
        t.start()
        flash("Manual Product Sync (Odoo -> Shopify) started in background.", "success")
    except Exception as e:
        flash(f"Failed to start sync: {str(e)}", "error")
        logging.error(f"Manual sync trigger failed: {e}")
        
    return redirect(url_for('index'))

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Placeholder for saving configs logic
        product_dir = request.form.get('product_sync_direction')
        logging.info(f"User changed sync direction to: {product_dir}")
        # In a real app, save 'product_dir' to DB here
        pass

    # Load current settings (Mock defaults for display)
    current_settings = {
        "product_sync_direction": get_config('product_sync_direction', 'bidirectional'),
    }

    html_template = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Odoo-Shopify Sync Manager</title>
        <style>
            body { font-family: sans-serif; max-width: 800px; margin: 40px auto; padding: 20px; line-height: 1.6; }
            .card { border: 1px solid #ddd; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            select, button { font-size: 16px; padding: 8px; }
            .flash { padding: 10px; margin-bottom: 20px; border-radius: 4px; }
            .flash.success { background-color: #d4edda; color: #155724; }
            .flash.error { background-color: #f8d7da; color: #721c24; }
            label { font-weight: bold; display: block; margin-bottom: 8px; }
            small { color: #666; display: block; margin-top: 5px; }
        </style>
    </head>
    <body>
        <h1>Integration Dashboard</h1>
        
        <!-- Flash Messages -->
        {% with messages = get_flashed_messages(with_categories=true) %}
          {% if messages %}
            <div>
            {% for category, message in messages %}
              <div class="flash {{ category }}">{{ message }}</div>
            {% endfor %}
            </div>
          {% endif %}
        {% endwith %}
        
        <h2>Product Sync Settings</h2>
        <div class="card">
            <form method="POST">
                <label>Product Sync Direction:</label>
                <select name="product_sync_direction">
                    <option value="bidirectional" {% if settings.product_sync_direction == 'bidirectional' %}selected{% endif %}>Bidirectional (Merge)</option>
                    <option value="shopify_to_odoo" {% if settings.product_sync_direction == 'shopify_to_odoo' %}selected{% endif %}>Shopify to Odoo (Create Missing in Odoo)</option>
                    <option value="odoo_to_shopify" {% if settings.product_sync_direction == 'odoo_to_shopify' %}selected{% endif %}>Odoo to Shopify (Master)</option>
                </select>
                <small>If Bidirectional/Shopify-to-Odoo: Incoming orders with unknown SKUs will create products in Odoo.</small>
                <small>If Bidirectional/Odoo-to-Shopify: Products created/updated in Odoo sync to Shopify every 30 mins.</small>
                <br>
                <button type="submit">Save Settings</button>
            </form>
            
            <hr>
            
            <h3>Manual Actions</h3>
            <p>Force sync newly added products from Odoo to Shopify immediately.</p>
            <form action="{{ url_for('manual_sync_products') }}" method="POST">
                <button type="submit" style="background-color: #007bff; color: white; border: none; cursor: pointer;">
                    Sync Odoo Products to Shopify Now
                </button>
            </form>
        </div>
    </body>
    </html>
    '''

    return render_template_string(html_template, settings=current_settings)

# --- Scheduler ---

def run_schedule():
    # Schedule Product Sync every 30 minutes
    schedule.every(30).minutes.do(sync_odoo_products_to_shopify)
    
    logging.info("Scheduler started.")
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    # Start scheduler in background thread
    t = threading.Thread(target=run_schedule, daemon=True) # daemon=True ensures thread dies when main app dies
    t.start()
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
