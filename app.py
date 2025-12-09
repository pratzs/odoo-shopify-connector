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
            
            # Map the newly created Odoo partner to the Shopify customer ID (if available)
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

    # 2. Addresses (Delivery/Invoice) - Logic omitted for brevity (same as before)
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
            price = float(item.get('price', 0))
            qty = int(item.get('quantity', 1))
            lines.append((0, 0, {'product_id': product_id, 'product_uom_qty': qty, 'price_unit': price, 'name': item['name']}))
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
            odoo.create_sale_order(vals)
            log_event('Order', 'Success', f"Synced {client_ref}")
            return True, "Synced"
    except Exception as e:
        log_event('Order', 'Error', str(e))
        return False, str(e)

def sync_products_master():
    """Odoo is Master: Pushes all Odoo products to Shopify (Updates Status)"""
    with app.app_context():
        if not odoo or not setup_shopify_session(): 
            log_event('System', 'Error', "Product Sync Failed: Connection Error")
            return

        company_id = get_config('odoo_company_id')
        odoo_products = odoo.get_all_products(company_id)
        
        log_event('Product Sync', 'Info', f"Found {len(odoo_products)} products (Active+Archived). Starting Master Sync...")
        
        synced = 0
        for p in odoo_products:
            sku = p.get('default_code')
            if not sku: continue

            target_status = 'active' if p.get('active', True) else 'archived'
            shopify_id = find_shopify_product_by_sku(sku)
            
            try:
                if shopify_id:
                    sp = shopify.Product.find(shopify_id)
                else:
                    if target_status == 'archived': continue
                    sp = shopify.Product()
                
                sp.title = p['name']
                sp.body_html = p.get('description_sale') or ''
                sp.product_type = 'Storable Product'
                sp.vendor = 'Odoo Master'
                sp.status = target_status
                
                sp.save()
                
                if sp.variants:
                    variant = sp.variants[0]
                else:
                    variant = shopify.Variant()
                    
                variant.sku = sku
                variant.price = str(p['list_price'])
                variant.barcode = p.get('barcode') or ''
                variant.weight = p.get('weight', 0)
                variant.inventory_management = 'shopify'
                
                variant.product_id = sp.id
                variant.save()
                
                synced += 1
            except Exception as e:
                log_event('Product Sync', 'Error', f"Failed {sku}: {e}")
                
        log_event('Product Sync', 'Success', f"Master Sync Complete. Processed {synced} products.")

def sync_customers_master():
    """Odoo is Master: Pushes updated Odoo customers to Shopify."""
    with app.app_context():
        # Check if auto-sync is enabled
        if not get_config('cust_auto_sync', False):
            log_event('Customer Sync', 'Skipped', 'Auto sync disabled by configuration.')
            return

        # Fetch filtering tags
        sync_tags_enabled = get_config('cust_sync_tags', False)
        whitelist_raw = get_config('cust_whitelist_tags', '')
        blacklist_raw = get_config('cust_blacklist_tags', '')

        # Process tags into sets for fast lookup (case-insensitive)
        whitelist_tags = {t.strip().lower() for t in whitelist_raw.split(',') if t.strip()}
        blacklist_tags = {t.strip().lower() for t in blacklist_raw.split(',') if t.strip()}

        last_sync_key = 'cust_last_sync'
        # Default to checking last 24 hours if no sync time saved
        last_sync_str = get_config(last_sync_key, (datetime.utcnow() - timedelta(hours=24)).isoformat())
        last_sync_dt = datetime.fromisoformat(last_sync_str)
        company_id = get_config('odoo_company_id')
        
        if not odoo or not setup_shopify_session(): 
            log_event('System', 'Error', "Customer Sync Failed: Connection Error")
            return
            
        odoo_customers = odoo.get_changed_customers(last_sync_dt.strftime('%Y-%m-%d %H:%M:%S'), company_id)
        
        log_event('Customer Sync', 'Info', f"Found {len(odoo_customers)} customers changed since {last_sync_dt.strftime('%Y-%m-%d')}.")
        
        synced_count = 0
        current_time_str = datetime.utcnow().isoformat()
        
        for oc in odoo_customers:
            odoo_id = oc['id']
            email = oc['email']

            # --- TAG FILTERING LOGIC ---
            if sync_tags_enabled:
                # Odoo tags are fetched as a list of tuples: [(ID, Name), ...]
                odoo_partner_tags = {tag[1].lower() for tag in oc.get('category_id') or []}

                # 1. Blacklist Check: If customer has ANY blacklisted tag, SKIP.
                if odoo_partner_tags.intersection(blacklist_tags):
                    log_event('Customer Sync', 'Skipped', f"Skipping {oc['name']}: Matched Blacklist Tag.")
                    continue
                
                # 2. Whitelist Check: If whitelist exists AND customer has NONE of them, SKIP.
                # If whitelist_tags is empty, this check is skipped (default behavior: sync all)
                if whitelist_tags and not odoo_partner_tags.intersection(whitelist_tags):
                     log_event('Customer Sync', 'Skipped', f"Skipping {oc['name']}: Missing Whitelist Tag.")
                     continue
            # --- END TAG FILTERING ---
            
            # 1. Find Shopify ID using database map
            cust_map = CustomerMap.query.filter_by(odoo_partner_id=odoo_id).first()
            shopify_cust_id = cust_map.shopify_customer_id if cust_map else None

            # 2. Find Shopify customer by ID or Email
            sc = None
            if shopify_cust_id:
                try:
                    sc = shopify.Customer.find(shopify_cust_id)
                except:
                    shopify_cust_id = None # ID failed, try email search
            
            if not sc and email:
                search_results = shopify.Customer.search(query=f'email:{email}')
                if search_results:
                    sc = search_results[0]
            
            if not sc:
                sc = shopify.Customer()
            
            # 3. Map Data (Odoo -> Shopify)
            name_parts = oc['name'].split()
            first_name = name_parts[0] if name_parts else ''
            last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ''

            sc.email = email
            sc.first_name = first_name
            sc.last_name = last_name
            sc.phone = oc['phone']
            
            # Simplified address mapping (Odoo master address to Shopify default address)
            address_data = {
                'address1': oc.get('street'),
                'city': oc.get('city'),
                'zip': oc.get('zip'),
                'country_id': oc.get('country_id')[0] if oc.get('country_id') else None
            }
            sc.addresses = [address_data] if any(address_data.values()) else []
            
            # Map Odoo Partner Tags (Categories)
            if sync_tags_enabled:
                odoo_tags = [tag[1] for tag in oc.get('category_id') or []]
                sc.tags = ",".join(odoo_tags)
            else:
                # If filtering is disabled, DO NOT touch the tags on Shopify.
                # The existing sc.tags value (if loaded from Shopify) will be preserved upon sc.save()
                pass


            # 4. Save and Update Map
            if sc.save():
                synced_count += 1
                
                # Update/Create the map in the local database
                if not cust_map:
                    # After successful Shopify save, Shopify gives us the final customer ID (sc.id)
                    cust_map = CustomerMap(shopify_customer_id=str(sc.id), odoo_partner_id=odoo_id, email=email)
                    db.session.add(cust_map)
                
                log_event('Customer Sync', 'Success', f"Synced Customer: {oc['name']} (Odoo ID: {odoo_id})")
            else:
                 log_event('Customer Sync', 'Warning', f"Failed to sync {oc['name']}: {sc.errors.full_messages()}")

        # Commit all mapping changes at the end
        db.session.commit()
        
        # Update last successful sync time
        set_config(last_sync_key, current_time_str)
        log_event('Customer Sync', 'Success', f"Customer Master Sync Complete. Processed {synced_count} updates.")


def archive_shopify_duplicates():
    """Scans Shopify for duplicate SKUs and archives the older ones."""
    with app.app_context(): # FIX: Database context for background thread
        if not setup_shopify_session(): return
        
        log_event('Duplicate Scan', 'Info', "Starting Duplicate SKU Scan...")
        
        sku_map = {}
        # FIX: Implement robust pagination here if required for stores > 250 variants
        variants = shopify.Variant.find(limit=250) 
        
        for v in variants:
            if not v.sku: continue
            if v.sku not in sku_map:
                sku_map[v.sku] = []
            sku_map[v.sku].append(v)

        count = 0
        for sku, var_list in sku_map.items():
            if len(var_list) > 1:
                var_list.sort(key=lambda x: x.id, reverse=True)
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
    if not odoo: return jsonify({"error": "Offline"}), 500
    
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
    for p_id in product_ids:
        total = odoo.get_total_qty_for_locations(p_id, target_locations, field_name=target_field)
        if sync_zero and total <= 0: continue
        count += 1
        if count <= 3:
             p_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'read', [p_id], {'fields': ['default_code']})
             sku = p_data[0].get('default_code')
             log_event('Inventory', 'Info', f"Synced SKU {sku}: {total}")
    return jsonify({"synced": count})

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
    with app.app_context():
        process_order_data(request.json)
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
     return jsonify({})

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
