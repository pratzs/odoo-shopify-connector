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
from models import db, ProductMap, SyncLog, AppSetting, CustomerMap
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
    if not setup_shopify_session(): return None
    
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

def get_shopify_variant_inv_by_sku(sku):
    """Fetches Variant ID, Inv Item ID, and Current Qty for efficient comparison"""
    if not setup_shopify_session(): return None
    query = """
    {
      productVariants(first: 1, query: "sku:%s") {
        edges {
          node {
            legacyResourceId
            inventoryItem {
              legacyResourceId
            }
            inventoryQuantity
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
            node = edges[0]['node']
            return {
                'variant_id': node['legacyResourceId'],
                'inventory_item_id': node['inventoryItem']['legacyResourceId'],
                'qty': node['inventoryQuantity']
            }
    except Exception as e:
        print(f"GraphQL Inv Error: {e}")
    return None

# --- CORE LOGIC ---

def process_order_data(data):
    """Syncs order. Checks for duplicates before creating new products."""
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
            
            shopify_cust_id = str(data.get('customer', {}).get('id'))
            if shopify_cust_id:
                cust_map = CustomerMap(shopify_customer_id=shopify_cust_id, odoo_partner_id=partner_id, email=email)
                db.session.add(cust_map)
                db.session.commit()

        except Exception as e:
            return False, f"Customer Error: {e}"
    
    partner_id = extract_id(partner['parent_id'][0] if partner.get('parent_id') else partner['id'])
    
    # Salesperson Lookup
    sales_rep_id = odoo.get_partner_salesperson(partner_id)
    if not sales_rep_id: sales_rep_id = odoo.uid

    # 2. Addresses (Delivery/Invoice)
    shipping_id = partner_id # Placeholder for full logic
    invoice_id = partner_id  # Placeholder for full logic

    # 3. Build Lines & Handle Missing Products
    lines = []
    for item in data.get('line_items', []):
        sku = item.get('sku')
        if not sku: continue

        # STRICT SEARCH: Only find Active products
        product_id = odoo.search_product_by_sku(sku, company_id)
        
        if not product_id:
            # Check if it exists as ARCHIVED
            archived_id = odoo.check_product_exists_by_sku(sku, company_id)
            
            if archived_id:
                log_event('Order', 'Warning', f"Skipped SKU {sku}: Product is Archived in Odoo.")
                continue 
            
            # If completely missing (Active OR Archived), Create it
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
                product_id = odoo.search_product_by_sku(sku, company_id) 
            except Exception as e:
                log_event('Product', 'Error', f"Failed to create SKU {sku}: {e}")

        if product_id:
            # --- YOUR EXACT STABLE LOGIC ---
            price = float(item.get('price', 0))
            qty = int(item.get('quantity', 1))
            disc = float(item.get('total_discount', 0))
            
            # Original percentage calculation
            pct = (disc / (price * qty)) * 100 if price > 0 else 0.0
            
            lines.append((0, 0, {
                'product_id': product_id, 
                'product_uom_qty': qty, 
                'price_unit': price, 
                'name': item['name'], 
                'discount': pct
            }))
        else:
            log_event('Order', 'Warning', f"Skipped line {sku}: Could not create/find active product.")

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
            return True, "Updated"
        else:
            # FIXED: Pass context to FORCE Odoo to use our price (4.08) not Price List (4.49)
            odoo.create_sale_order(vals, context={'manual_price': True})
            log_event('Order', 'Success', f"Synced {client_ref}")
            return True, "Synced"
    except Exception as e:
        log_event('Order', 'Error', str(e))
        return False, str(e)

# --- PRODUCTS SYNC ---
def sync_products_master():
    """Odoo -> Shopify Product Sync (Efficient: Only Updates if Changed)"""
    with app.app_context():
        if not odoo or not setup_shopify_session(): 
            log_event('System', 'Error', "Product Sync Failed: Connection Error")
            return

        company_id = get_config('odoo_company_id')
        odoo_products = odoo.get_all_products(company_id)
        
        log_event('Product Sync', 'Info', f"Found {len(odoo_products)} products. Starting Master Sync...")
        
        synced = 0
        for p in odoo_products:
            sku = p.get('default_code')
            if not sku: continue

            target_status = 'active' if p.get('active', True) else 'archived'
            
            # --- ARCHIVED LOGIC FIX ---
            if target_status == 'archived':
                shopify_id = find_shopify_product_by_sku(sku)
                if shopify_id:
                    try:
                        sp = shopify.Product.find(shopify_id)
                        if sp.status != 'archived':
                            sp.status = 'archived'
                            sp.save()
                            log_event('Product Sync', 'Info', f"Archived {sku} in Shopify.")
                    except: pass
                continue 

            # --- ACTIVE PRODUCT LOGIC ---
            shopify_id = find_shopify_product_by_sku(sku)
            try:
                if shopify_id:
                    sp = shopify.Product.find(shopify_id)
                else:
                    sp = shopify.Product()
                
                # --- CHANGE DETECTION & MAPPING ---
                product_changed = False
                
                # Title
                if sp.title != p['name']:
                    sp.title = p['name']
                    product_changed = True

                # Description
                odoo_desc = p.get('description_sale') or ''
                # Handle None vs empty string
                if (sp.body_html or '') != odoo_desc:
                    sp.body_html = odoo_desc
                    product_changed = True
                
                # Product Type Mapping
                categ_name = odoo.get_public_category_name(p.get('public_categ_ids'))
                target_type = categ_name if categ_name else 'Storable Product'
                if sp.product_type != target_type:
                    sp.product_type = target_type
                    product_changed = True
                
                # Vendor Mapping (UPDATED: Use First Word of Product Title)
                product_title = p.get('name', '')
                target_vendor = product_title.split()[0] if product_title else 'Odoo Master'
                if sp.vendor != target_vendor:
                    sp.vendor = target_vendor
                    product_changed = True

                # Status
                if sp.status != target_status:
                    sp.status = target_status
                    product_changed = True
                
                # Only save main product if changes detected or it's new
                if product_changed or not shopify_id:
                    sp.save()
                
                # Variant Logic
                if sp.variants:
                    variant = sp.variants[0]
                else:
                    variant = shopify.Variant()
                
                variant_changed = False
                
                if variant.sku != sku:
                    variant.sku = sku
                    variant_changed = True
                
                # Price Check
                target_price = str(p['list_price'])
                if variant.price != target_price:
                    variant.price = target_price
                    variant_changed = True

                # Barcode Check
                target_barcode = p.get('barcode', 0) or ''
                if str(variant.barcode or '') != str(target_barcode):
                    variant.barcode = str(target_barcode)
                    variant_changed = True
                
                # Weight Check (Float comparison)
                try:
                    if float(variant.weight or 0) != float(p.get('weight', 0)):
                        variant.weight = p.get('weight', 0)
                        variant_changed = True
                except:
                    variant.weight = p.get('weight', 0)
                    variant_changed = True

                if variant.inventory_management != 'shopify':
                    variant.inventory_management = 'shopify'
                    variant_changed = True
                
                if str(variant.product_id) != str(sp.id):
                    variant.product_id = sp.id
                    variant_changed = True

                if variant_changed or not shopify_id:
                    variant.save()
                
                # Inventory Sync
                if SHOPIFY_LOCATION_ID and variant.inventory_item_id:
                    # Optimized: Only update if different
                    # We reuse variant object if available, otherwise fetch
                    # But finding exact quantity via REST is messy.
                    # We will update inventory in the optimized sync_inventory loop
                    # OR we can do it here unconditionally to be safe for master sync.
                    # Given Master Sync runs once a day, forcing update is acceptable here.
                    qty = int(p.get('qty_available', 0))
                    try:
                        shopify.InventoryLevel.set(
                            location_id=SHOPIFY_LOCATION_ID,
                            inventory_item_id=variant.inventory_item_id,
                            available=qty
                        )
                    except: pass

                # Image Sync (Only if missing in Shopify)
                if not sp.images:
                    img_data = odoo.get_product_image(p['id'])
                    if img_data:
                        image = shopify.Image(prefix_options={'product_id': sp.id})
                        image.attachment = img_data
                        image.save()
                
                # Metafield Sync (Vendor Code)
                vendor_code = odoo.get_vendor_product_code(p['product_tmpl_id'][0])
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
                synced += 1
            except Exception as e:
                log_event('Product Sync', 'Error', f"Failed {sku}: {e}")
        log_event('Product Sync', 'Success', f"Master Sync Complete. Processed {synced} active products.")

def sync_customers_master():
    """Odoo -> Shopify Customer Sync"""
    with app.app_context():
        if not get_config('cust_auto_sync', False):
            log_event('Customer Sync', 'Skipped', 'Auto sync disabled.')
            return

        sync_tags_enabled = get_config('cust_sync_tags', False)
        whitelist_raw = get_config('cust_whitelist_tags', '')
        blacklist_raw = get_config('cust_blacklist_tags', '')
        whitelist_tags = {t.strip().lower() for t in whitelist_raw.split(',') if t.strip()}
        blacklist_tags = {t.strip().lower() for t in blacklist_raw.split(',') if t.strip()}

        last_sync_key = 'cust_last_sync'
        last_sync_str = get_config(last_sync_key, (datetime.utcnow() - timedelta(hours=24)).isoformat())
        last_sync_dt = datetime.fromisoformat(last_sync_str)
        company_id = get_config('odoo_company_id')
        
        if not odoo or not setup_shopify_session(): return
        odoo_customers = odoo.get_changed_customers(last_sync_dt.strftime('%Y-%m-%d %H:%M:%S'), company_id)
        log_event('Customer Sync', 'Info', f"Found {len(odoo_customers)} customers changed.")
        
        synced_count = 0
        current_time_str = datetime.utcnow().isoformat()
        
        for oc in odoo_customers:
            odoo_id = oc['id']
            email = oc['email']

            if sync_tags_enabled:
                odoo_partner_tags = {tag[1].lower() for tag in oc.get('category_id') or []}
                if odoo_partner_tags.intersection(blacklist_tags): continue
                if whitelist_tags and not odoo_partner_tags.intersection(whitelist_tags): continue
            
            cust_map = CustomerMap.query.filter_by(odoo_partner_id=odoo_id).first()
            shopify_cust_id = cust_map.shopify_customer_id if cust_map else None

            sc = None
            if shopify_cust_id:
                try: sc = shopify.Customer.find(shopify_cust_id)
                except: shopify_cust_id = None
            
            if not sc and email:
                search_results = shopify.Customer.search(query=f'email:{email}')
                if search_results: sc = search_results[0]
            
            if not sc: sc = shopify.Customer()
            
            name_parts = oc['name'].split()
            sc.email = email
            sc.first_name = name_parts[0] if name_parts else ''
            sc.last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ''
            sc.phone = oc['phone']
            address_data = {'address1': oc.get('street'), 'city': oc.get('city'), 'zip': oc.get('zip'), 'country_id': oc.get('country_id')[0] if oc.get('country_id') else None}
            sc.addresses = [address_data] if any(address_data.values()) else []
            
            if sync_tags_enabled:
                odoo_tags = [tag[1] for tag in oc.get('category_id') or []]
                sc.tags = ",".join(odoo_tags)
                
            if sc.save():
                synced_count += 1
                if not cust_map:
                    cust_map = CustomerMap(shopify_customer_id=str(sc.id), odoo_partner_id=odoo_id, email=email)
                    db.session.add(cust_map)
                log_event('Customer Sync', 'Success', f"Synced {oc['name']}")
        
        db.session.commit()
        set_config(last_sync_key, current_time_str)
        log_event('Customer Sync', 'Success', f"Customer Sync Complete. Processed {synced_count} updates.")

def archive_shopify_duplicates():
    with app.app_context():
        if not setup_shopify_session(): return
        log_event('Duplicate Scan', 'Info', "Starting Scan...")
        sku_map = {}
        variants = shopify.Variant.find(limit=250)
        for v in variants:
            if not v.sku: continue
            if v.sku not in sku_map: sku_map[v.sku] = []
            sku_map[v.sku].append(v)
        count = 0
        for sku, var_list in sku_map.items():
            if len(var_list) > 1:
                var_list.sort(key=lambda x: x.id, reverse=True)
                for dup in var_list[1:]:
                    try:
                        prod = shopify.Product.find(dup.product_id)
                        prod.status = 'archived'
                        prod.save()
                        count += 1
                    except Exception as e:
                        print(f"Archive fail: {e}")
        if count == 0: log_event('Duplicate Scan', 'Success', "Clean!")
        else: log_event('Duplicate Scan', 'Success', f"Archived {count} duplicate products.")

# --- ROUTES ---

@app.route('/')
def dashboard():
    try:
        logs_orders = SyncLog.query.filter(SyncLog.entity.in_(['Order', 'Order Cancel'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_inventory = SyncLog.query.filter_by(entity='Inventory').order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_products = SyncLog.query.filter(SyncLog.entity.in_(['Product', 'Product Sync', 'Duplicate Scan'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_customers = SyncLog.query.filter(SyncLog.entity.in_(['Customer', 'Customer Sync'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_system = SyncLog.query.filter(SyncLog.entity.notin_(['Order', 'Order Cancel', 'Inventory', 'Customer', 'Product', 'Product Sync', 'Duplicate Scan', 'Customer Sync'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
    except:
        logs_orders = logs_inventory = logs_products = logs_customers = logs_system = []
    
    current_settings = {
        "odoo_company_id": get_config('odoo_company_id', None),
        "field": get_config('inventory_field', 'qty_available'),
        "sync_zero": get_config('sync_zero_stock', False),
        "combine_committed": get_config('combine_committed', False),
        "cust_direction": get_config('cust_direction', 'bidirectional'),
        "cust_auto_sync": get_config('cust_auto_sync', True),
        "cust_sync_tags": get_config('cust_sync_tags', False),
        "cust_whitelist_tags": get_config('cust_whitelist_tags', ''),
        "cust_blacklist_tags": get_config('cust_blacklist_tags', '')
    }
    odoo_status = True if odoo else False
    return render_template('dashboard.html', 
                           logs_orders=logs_orders, logs_inventory=logs_inventory, logs_products=logs_products,
                           logs_customers=logs_customers, logs_system=logs_system,
                           odoo_status=odoo_status, current_settings=current_settings)

# --- NEW ROUTE FOR LIVE LOGS ---
@app.route('/live_logs')
def live_logs():
    return render_template('live_logs.html')

@app.route('/api/logs/live', methods=['GET'])
def api_live_logs():
    """Provides logs for the live viewer directly from DB"""
    try:
        # Fetch latest 100 logs
        logs = SyncLog.query.order_by(SyncLog.timestamp.desc()).limit(100).all()
        
        data = []
        for log in logs:
            # Map DB status to UI types
            msg_type = 'info'
            status_lower = (log.status or '').lower()
            if 'error' in status_lower or 'fail' in status_lower: msg_type = 'error'
            elif 'success' in status_lower: msg_type = 'success'
            elif 'warning' in status_lower or 'skip' in status_lower: msg_type = 'warning'
            
            data.append({
                'id': log.id,
                'timestamp': log.timestamp.isoformat(),
                'message': f"[{log.entity}] {log.message}", 
                'type': msg_type,
                'details': log.status
            })
        return jsonify(data)
    except Exception as e:
        return jsonify([])

@app.route('/sync/products/master', methods=['POST'])
def trigger_master_sync():
    threading.Thread(target=sync_products_master).start()
    return jsonify({"message": "Master Product Sync Started (Odoo -> Shopify)"})

@app.route('/sync/products/archive_duplicates', methods=['POST'])
def trigger_duplicate_scan():
    threading.Thread(target=archive_shopify_duplicates).start()
    return jsonify({"message": "Duplicate Scan Started"})

@app.route('/sync/customers/master', methods=['POST'])
def trigger_customer_master_sync():
    threading.Thread(target=sync_customers_master).start()
    return jsonify({"message": "Master Customer Sync Started (Odoo -> Shopify)"})

@app.route('/api/odoo/companies', methods=['GET'])
def api_get_companies():
    if not odoo: return jsonify({"error": "Odoo Offline"}), 500
    try: return jsonify(odoo.get_companies())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/api/odoo/locations', methods=['GET'])
def api_get_locations():
    if not odoo: return jsonify({"error": "Odoo Offline"}), 500
    try:
        company_id = request.args.get('company_id')
        locs = odoo.get_locations(company_id)
        return jsonify(locs)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/settings/save', methods=['POST'])
def api_save_settings():
    data = request.json
    set_config('inventory_locations', data.get('locations', []))
    set_config('inventory_field', data.get('field', 'qty_available'))
    set_config('sync_zero_stock', data.get('sync_zero', False))
    set_config('combine_committed', data.get('combine_committed', False))
    set_config('odoo_company_id', data.get('company_id'))
    set_config('cust_direction', data.get('cust_direction'))
    set_config('cust_auto_sync', data.get('cust_auto_sync'))
    # UPDATED: Save the two new tag configuration keys
    set_config('cust_sync_tags', data.get('cust_sync_tags'))
    set_config('cust_whitelist_tags', data.get('cust_whitelist_tags', ''))
    set_config('cust_blacklist_tags', data.get('cust_blacklist_tags', ''))
    return jsonify({"message": "Settings Saved"})

@app.route('/sync/inventory', methods=['GET'])
def sync_inventory():
    """Optimized Inventory Sync: Only updates if Shopify differs from Odoo"""
    if not odoo or not setup_shopify_session(): return jsonify({"error": "Offline"}), 500
    
    with app.app_context():
        target_locations = get_config('inventory_locations', [])
        target_field = get_config('inventory_field', 'qty_available')
        sync_zero = get_config('sync_zero_stock', False)
        company_id = get_config('odoo_company_id', None)
        
        if not company_id:
            try:
                u = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'res.users', 'read', [[odoo.uid]], {'fields': ['company_id']})
                if u: company_id = u[0]['company_id'][0]
            except: pass

        last_run = datetime.utcnow() - timedelta(minutes=35)
        try: 
            product_ids = odoo.get_changed_products(str(last_run), company_id)
        except: return jsonify({"error": "Read Failed"}), 500
        
        count = 0
        updates = 0
        for p_id in product_ids:
            # 1. Get Odoo Total
            total_odoo = int(odoo.get_total_qty_for_locations(p_id, target_locations, field_name=target_field))
            if sync_zero and total_odoo <= 0: continue
            
            p_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'read', [p_id], {'fields': ['default_code']})
            sku = p_data[0].get('default_code')
            
            if not sku: continue
            
            # 2. Get Shopify Current State
            shopify_info = get_shopify_variant_inv_by_sku(sku)
            if not shopify_info: continue
            
            # 3. Compare
            if int(shopify_info['qty']) != total_odoo:
                # 4. Update if different
                try:
                    shopify.InventoryLevel.set(
                        location_id=SHOPIFY_LOCATION_ID,
                        inventory_item_id=shopify_info['inventory_item_id'],
                        available=total_odoo
                    )
                    updates += 1
                    log_event('Inventory', 'Info', f"Updated SKU {sku}: {shopify_info['qty']} -> {total_odoo}")
                except Exception as e:
                    print(f"Inv Set Error {sku}: {e}")
            
            count += 1
            
        return jsonify({"synced": count, "updates": updates})

@app.route('/sync/orders/manual', methods=['GET'])
def manual_order_fetch():
    url = f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders.json?status=open&limit=10"
    headers = {"X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN')}
    res = requests.get(url, headers=headers)
    orders = res.json().get('orders', []) if res.status_code == 200 else []
    
    mapped_orders = []
    for o in orders:
        status = "Not Synced"
        try:
            exists = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search', [[['client_order_ref', 'ilike', o['name']]]])
            if exists: status = "Synced"
        except: pass
        if o.get('cancelled_at'): status = "Cancelled (Skipped)"
        mapped_orders.append({'id': o['id'], 'name': o['name'], 'date': o['created_at'], 'total': o['total_price'], 'odoo_status': status})
    return jsonify({"orders": mapped_orders})

@app.route('/sync/orders/import_batch', methods=['POST'])
def import_selected_orders():
    ids = request.json.get('order_ids', [])
    headers = {"X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN')}
    synced = 0
    for oid in ids:
        res = requests.get(f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders/{oid}.json", headers=headers)
        if res.status_code == 200:
            success, _ = process_order_data(res.json().get('order'))
            if success: synced += 1
    return jsonify({"message": f"Batch Complete. Synced: {synced}"})

@app.route('/webhook/orders', methods=['POST'])
@app.route('/webhook/orders/updated', methods=['POST'])
def order_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    with app.app_context(): process_order_data(request.json)
    return "Received", 200

@app.route('/webhook/orders/cancelled', methods=['POST'])
def order_cancelled_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    with app.app_context():
        data = request.json
        client_ref = f"ONLINE_{data.get('name')}"
        order_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search', [[['client_order_ref', '=', client_ref], ['state', '!=', 'cancel']]])
        if order_ids:
            odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'action_cancel', [order_ids])
            log_event('Order Cancel', 'Success', f"Cancelled {client_ref}")
    return "Cancelled", 200

@app.route('/webhook/refunds', methods=['POST'])
def refund_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    log_event('Refund', 'Info', "Refund webhook received")
    return "Received", 200

@app.route('/test/simulate_order', methods=['POST'])
def test_sim_dummy():
     return jsonify({"message": "Connection OK"})

@app.route('/sync/order_status', methods=['GET'])
def sync_order_status():
    return jsonify({"status": "Checked"})

def run_schedule():
    # Master Product Sync daily
    schedule.every(1).days.do(sync_products_master)
    # Master Customer Sync daily
    schedule.every(1).days.do(sync_customers_master)
    # Monthly Duplicate Scan
    schedule.every(30).days.do(archive_shopify_duplicates)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    t = threading.Thread(target=run_schedule, daemon=True)
    t.start()
    app.run(debug=True)
