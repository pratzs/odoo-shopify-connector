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

# --- DB INIT ---
with app.app_context():
    try: 
        db.create_all()
        print("Database tables created/verified.")
    except Exception as e: 
        print(f"CRITICAL DB INIT ERROR: {e}")

# --- GLOBAL LOCKS FOR CONCURRENCY CONTROL ---
# This prevents the same order from being processed by multiple threads/webhooks simultaneously
order_processing_lock = threading.Lock()
active_processing_ids = set()

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
        log = SyncLog(
            entity=entity, 
            status=status, 
            message=message, 
            timestamp=datetime.utcnow()
        )
        db.session.add(log)
        db.session.commit()
    except Exception as e: 
        print(f"DB LOG ERROR: {e}")
        db.session.rollback()

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

def process_product_data(data):
    """
    Handles Shopify Product Webhooks (Update Only).
    1. IGNORES new products (Does NOT create in Odoo).
    2. Syncs 'Product Type' -> Odoo 'Ecommerce Category' for existing active products.
    3. Does NOT overwrite Price/Stock on updates (Odoo is Master).
    """
    product_type = data.get('product_type', '')
    
    # 1. Resolve Category ID (Shopify Type -> Odoo Public Category)
    cat_id = None
    if product_type:
        try:
            cat_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.public.category', 'search', [[['name', '=', product_type]]])
            if cat_ids:
                cat_id = cat_ids[0]
            else:
                cat_id = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                    'product.public.category', 'create', [{'name': product_type}])
        except Exception as e:
            print(f"Category Logic Error: {e}")

    # 2. Iterate Variants (Shopify Product -> Odoo Variants)
    variants = data.get('variants', [])
    processed_count = 0
    company_id = get_config('odoo_company_id')
    
    for v in variants:
        sku = v.get('sku')
        if not sku: continue # Skip products without SKU
        
        # Check if exists in Odoo (Active Only)
        product_id = odoo.search_product_by_sku(sku, company_id)
        
        if product_id:
            # --- UPDATE LOGIC (Category Only) ---
            # We ONLY update the category to match Shopify Product Type.
            if cat_id:
                try:
                    # Check current category to avoid redundant writes
                    current_prod = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                        'product.product', 'read', [[product_id]], {'fields': ['public_categ_ids']})
                    
                    current_cat_ids = current_prod[0].get('public_categ_ids', [])
                    
                    if cat_id not in current_cat_ids:
                        odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                            'product.product', 'write', [[product_id], {'public_categ_ids': [(4, cat_id)]}])
                        log_event('Product', 'Info', f"Webhook: Updated Category for {sku} to '{product_type}'")
                        processed_count += 1
                except Exception as e:
                    print(f"Webhook Update Error: {e}")
        else:
            # --- SKIP CREATION ---
            # Product missing or Archived. We strictly ignore creation from direct product webhooks.
            # Creation is only allowed via Sales Order (see process_order_data).
            pass

    return processed_count

def process_order_data(data):
    """Syncs order. UPDATES existing orders instead of skipping, preventing duplicates."""
    shopify_id = str(data.get('id', ''))
    shopify_name = data.get('name')
    
    # 1. CONCURRENCY CHECK
    with order_processing_lock:
        if shopify_id in active_processing_ids:
            log_event('Order', 'Skipped', f"Order {shopify_name} skipped (Concurrent process detected).")
            return False, "Skipped"
        active_processing_ids.add(shopify_id)

    try:
        email = data.get('email') or data.get('contact_email')
        client_ref = f"ONLINE_{shopify_name}"
        company_id = get_config('odoo_company_id')
        
        if not company_id and odoo:
            try:
                user_info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                    'res.users', 'read', [[odoo.uid]], {'fields': ['company_id']})
                if user_info: company_id = user_info[0]['company_id'][0]
            except: pass

        # 2. CHECK EXISTING (Store ID, do not return yet)
        existing_order_id = None
        try:
            existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
            if existing_ids:
                existing_order_id = existing_ids[0]
        except Exception as e: return False, f"Odoo Error: {str(e)}"

        # 3. Customer Resolution
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
                
                if shopify_id:
                    c_id = str(data.get('customer', {}).get('id'))
                    if c_id:
                        cust_map = CustomerMap(shopify_customer_id=c_id, odoo_partner_id=partner_id, email=email)
                        db.session.add(cust_map)
                        db.session.commit()

            except Exception as e:
                return False, f"Customer Error: {e}"
        
        partner_id = extract_id(partner['parent_id'][0] if partner.get('parent_id') else partner['id'])
        
        # Salesperson
        sales_rep_id = odoo.get_partner_salesperson(partner_id)
        if not sales_rep_id: sales_rep_id = odoo.uid

        # Addresses
        shipping_id = partner_id 
        invoice_id = partner_id 

        # 4. Build Lines
        lines = []
        for item in data.get('line_items', []):
            sku = item.get('sku')
            if not sku: continue

            # Search Product
            product_id = odoo.search_product_by_sku(sku, company_id)
            
            if not product_id:
                # Check Archived
                archived_id = odoo.check_product_exists_by_sku(sku, company_id)
                if archived_id:
                    log_event('Order', 'Warning', f"Skipped SKU {sku}: Product is Archived.")
                    continue 
                
                # Create if missing
                log_event('Product', 'Info', f"SKU {sku} missing. Creating...")
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
                disc = float(item.get('total_discount', 0))
                pct = (disc / (price * qty)) * 100 if price > 0 else 0.0
                
                lines.append((0, 0, {
                    'product_id': product_id, 
                    'product_uom_qty': qty, 
                    'price_unit': price, 
                    'name': item['name'], 
                    'discount': pct
                }))
            else:
                log_event('Order', 'Warning', f"Skipped line {sku}: Product not found/created.")

        if not lines: return False, "No valid lines"
        
        # 5. SYNC LOGIC (Create OR Update)
        if existing_order_id:
            # --- UPDATE PATH ---
            # Check status first
            order_info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'sale.order', 'read', [[existing_order_id]], {'fields': ['state']})
            state = order_info[0]['state'] if order_info else 'unknown'

            if state in ['done', 'cancel']:
                log_event('Order', 'Skipped', f"Order {client_ref} is {state}. Update skipped.")
                return True, "Skipped"
            
            # Update Strategy: (5,0,0) removes all existing lines, then we add the new ones
            update_vals = {
                'order_line': [(5, 0, 0)] + lines,
                'partner_shipping_id': shipping_id,
                'partner_invoice_id': invoice_id
            }
            
            try:
                odoo.update_sale_order(existing_order_id, update_vals)
                log_event('Order', 'Success', f"Updated {client_ref} (Revision)")
                return True, "Updated"
            except Exception as e:
                log_event('Order', 'Error', f"Update Failed: {e}")
                return False, str(e)

        else:
            # --- CREATE PATH ---
            vals = {
                'name': client_ref, 'client_order_ref': client_ref,
                'partner_id': partner_id, 'partner_invoice_id': invoice_id, 'partner_shipping_id': shipping_id,
                'order_line': lines, 
                'user_id': sales_rep_id,
                'state': 'draft'
            }
            if company_id: vals['company_id'] = int(company_id)
            
            try:
                odoo.create_sale_order(vals, context={'manual_price': True})
                log_event('Order', 'Success', f"Synced {client_ref}")
                return True, "Synced"
            except Exception as e:
                log_event('Order', 'Error', str(e))
                return False, str(e)

    finally:
        # ALWAYS release the lock for this ID
        with order_processing_lock:
            if shopify_id in active_processing_ids:
                active_processing_ids.remove(shopify_id)

def cleanup_shopify_products(odoo_active_skus):
    """
    Scans Shopify Products:
    1. Archives Orphans (Products in Shopify but not active in Odoo).
    2. Archives Duplicates (Multiple Shopify products sharing one SKU).
    """
    if not setup_shopify_session(): return
    
    seen_skus = set()
    products = shopify.Product.find(limit=250)
    
    # Handle pagination manually for now or rely on first batch for efficiency
    # For robust cleanup, loop through pages:
    page = products
    processed_count = 0
    archived_count = 0
    
    try:
        while page:
            for sp in page:
                processed_count += 1
                variant = sp.variants[0] if sp.variants else None
                if not variant or not variant.sku: continue
                
                sku = variant.sku
                needs_archive = False
                
                # Check 1: Orphan (Not in Odoo Active List)
                if sku not in odoo_active_skus:
                    needs_archive = True
                    log_event('System', 'Info', f"Cleanup: SKU {sku} not found in Odoo (Orphan). Archiving in Shopify.")
                
                # Check 2: Duplicate (Already seen in this scan)
                elif sku in seen_skus:
                    needs_archive = True
                    log_event('System', 'Info', f"Cleanup: SKU {sku} is a duplicate in Shopify. Archiving.")
                
                if needs_archive:
                    if sp.status != 'archived':
                        sp.status = 'archived'
                        sp.save()
                        archived_count += 1
                else:
                    seen_skus.add(sku)
            
            if page.has_next_page():
                page = page.next_page()
            else:
                break
    except Exception as e:
        print(f"Cleanup Error: {e}")
        
    if archived_count > 0:
        log_event('System', 'Success', f"Cleanup Complete. Archived {archived_count} products.")

# --- PRODUCTS SYNC (Master) ---
def sync_products_master():
    """Odoo -> Shopify Product Sync (Efficient: Only Updates if Changed)"""
    with app.app_context():
        if not odoo or not setup_shopify_session(): 
            log_event('System', 'Error', "Product Sync Failed: Connection Error")
            return

        company_id = get_config('odoo_company_id')
        # Fetch all Odoo products to build the Master SKU list
        odoo_products = odoo.get_all_products(company_id)
        
        # Collect SKUs of ACTIVE Odoo products for cleanup phase
        active_odoo_skus = set()
        
        log_event('Product Sync', 'Info', f"Found {len(odoo_products)} products. Starting Master Sync...")
        
        synced = 0
        for p in odoo_products:
            sku = p.get('default_code')
            if not sku: continue

            # HANDLE ARCHIVED IN ODOO
            if not p.get('active', True):
                # If archived in Odoo, ensure it is archived in Shopify
                # We do NOT update other details, just status.
                shopify_id = find_shopify_product_by_sku(sku)
                if shopify_id:
                    try:
                        sp = shopify.Product.find(shopify_id)
                        if sp.status != 'archived':
                            sp.status = 'archived'
                            sp.save()
                            log_event('Product Sync', 'Info', f"Archived {sku} in Shopify (Matched Odoo status).")
                    except: pass
                continue # Skip rest of loop for archived items

            # Add to active set for cleanup later
            active_odoo_skus.add(sku)

            # --- ACTIVE PRODUCT SYNC ---
            shopify_id = find_shopify_product_by_sku(sku)
            try:
                if shopify_id:
                    sp = shopify.Product.find(shopify_id)
                else:
                    sp = shopify.Product()
                
                product_changed = False
                
                # Title
                if sp.title != p['name']:
                    sp.title = p['name']
                    product_changed = True

                # Description
                odoo_desc = p.get('description_sale') or ''
                if (sp.body_html or '') != odoo_desc:
                    sp.body_html = odoo_desc
                    product_changed = True
                
                # --- SMART CATEGORY SYNC ---
                # 1. If Odoo Category is EMPTY -> Import from Shopify (One-time init)
                # 2. If Odoo Category is SET -> Export to Shopify (Odoo Master)
                
                odoo_categ_ids = p.get('public_categ_ids', [])
                
                if not odoo_categ_ids and sp.product_type:
                    # CASE 1: Odoo is empty, populate from Shopify (One-time)
                    try:
                        cat_name = sp.product_type
                        # Search/Create category in Odoo
                        cat_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                            'product.public.category', 'search', [[['name', '=', cat_name]]])
                        
                        cat_id = cat_ids[0] if cat_ids else None
                        if not cat_id:
                            cat_id = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                                'product.public.category', 'create', [{'name': cat_name}])
                        
                        odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                            'product.product', 'write', [[p['id']], {'public_categ_ids': [(4, cat_id)]}])
                        log_event('Product Sync', 'Info', f"Initialized Odoo Category for {sku} from Shopify: {cat_name}")
                    except Exception as e:
                        print(f"Category Import Error: {e}")
                
                elif odoo_categ_ids:
                    # CASE 2: Odoo has data, enforce Odoo as Master
                    odoo_cat_name = odoo.get_public_category_name(odoo_categ_ids)
                    # If Odoo has a category, make sure Shopify matches it
                    if odoo_cat_name and sp.product_type != odoo_cat_name:
                        sp.product_type = odoo_cat_name
                        product_changed = True

                # Vendor Mapping
                product_title = p.get('name', '')
                target_vendor = product_title.split()[0] if product_title else 'Odoo Master'
                if sp.vendor != target_vendor:
                    sp.vendor = target_vendor
                    product_changed = True

                # Status
                if sp.status != 'active':
                    sp.status = 'active'
                    product_changed = True
                
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
                
                target_price = str(p['list_price'])
                if variant.price != target_price:
                    variant.price = target_price
                    variant_changed = True

                target_barcode = p.get('barcode', 0) or ''
                if str(variant.barcode or '') != str(target_barcode):
                    variant.barcode = str(target_barcode)
                    variant_changed = True
                
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
                    qty = int(p.get('qty_available', 0))
                    try:
                        shopify.InventoryLevel.set(
                            location_id=SHOPIFY_LOCATION_ID,
                            inventory_item_id=variant.inventory_item_id,
                            available=qty
                        )
                    except: pass

                # Image Sync (Stronger Logic: If Odoo has one and Shopify has NONE, push it)
                img_data = odoo.get_product_image(p['id'])
                if img_data and not sp.images:
                    image = shopify.Image(prefix_options={'product_id': sp.id})
                    image.attachment = img_data
                    image.save()
                    log_event('Product Sync', 'Info', f" synced Image for {sku}")
                
                # Metafield Sync
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
        
        # --- CLEANUP PHASE ---
        # Now that we know what *should* be in Shopify (active_odoo_skus),
        # we check Shopify for anything else (Orphans/Duplicates) and archive them.
        cleanup_shopify_products(active_odoo_skus)
        
        log_event('Product Sync', 'Success', f"Master Sync Complete. Processed {synced} active products.")

def sync_categories_only():
    """Run ONE-TIME import of Categories from Shopify to Odoo for existing products."""
    with app.app_context():
        if not odoo or not setup_shopify_session(): 
            log_event('System', 'Error', "Category Sync Failed: Connection Error")
            return

        company_id = get_config('odoo_company_id')
        odoo_products = odoo.get_all_products(company_id)
        log_event('System', 'Info', f"Starting Category-Only Sync for {len(odoo_products)} products...")
        
        updated_count = 0
        
        for p in odoo_products:
            sku = p.get('default_code')
            if not sku: continue
            
            # Skip if Odoo already has a category
            if p.get('public_categ_ids'):
                continue

            # Skip Archived Products (User Request)
            if not p.get('active', True):
                continue

            # Fetch from Shopify
            shopify_id = find_shopify_product_by_sku(sku)
            if not shopify_id: continue
            
            try:
                sp = shopify.Product.find(shopify_id)
                product_type = sp.product_type
                
                if product_type:
                    # Search/Create category in Odoo
                    cat_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                        'product.public.category', 'search', [[['name', '=', product_type]]])
                    
                    cat_id = cat_ids[0] if cat_ids else None
                    if not cat_id:
                        cat_id = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                            'product.public.category', 'create', [{'name': product_type}])
                    
                    # Link to Product
                    odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                        'product.product', 'write', [[p['id']], {'public_categ_ids': [(4, cat_id)]}])
                    
                    updated_count += 1
            except Exception as e:
                print(f"Error syncing category for {sku}: {e}")
        
        log_event('System', 'Success', f"Category Sync Finished. Updated {updated_count} products.")

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

# --- INVENTORY SYNC (SPLIT LOGIC) ---
def perform_inventory_sync(lookback_minutes):
    """Core logic for inventory sync. Returns (checked_count, updated_count)"""
    if not odoo or not setup_shopify_session(): return 0, 0
    
    target_locations = get_config('inventory_locations', [])
    target_field = get_config('inventory_field', 'qty_available')
    sync_zero = get_config('sync_zero_stock', False)
    company_id = get_config('odoo_company_id', None)
    
    if not company_id:
        try:
            u = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'res.users', 'read', [[odoo.uid]], {'fields': ['company_id']})
            if u: company_id = u[0]['company_id'][0]
        except: pass

    last_run = datetime.utcnow() - timedelta(minutes=lookback_minutes)
    
    try: 
        product_ids = odoo.get_changed_products(str(last_run), company_id)
    except: 
        return 0, 0
    
    count = 0
    updates = 0
    for p_id in product_ids:
        # Get Odoo Total
        total_odoo = int(odoo.get_total_qty_for_locations(p_id, target_locations, field_name=target_field))
        if sync_zero and total_odoo <= 0: continue
        
        p_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'read', [p_id], {'fields': ['default_code']})
        sku = p_data[0].get('default_code')
        if not sku: continue
        
        shopify_info = get_shopify_variant_inv_by_sku(sku)
        if not shopify_info: continue
        
        if int(shopify_info['qty']) != total_odoo:
            try:
                shopify.InventoryLevel.set(location_id=SHOPIFY_LOCATION_ID, inventory_item_id=shopify_info['inventory_item_id'], available=total_odoo)
                updates += 1
                log_event('Inventory', 'Info', f"Updated SKU {sku}: {shopify_info['qty']} -> {total_odoo}")
            except Exception as e:
                print(f"Inv Error {sku}: {e}")
        count += 1
    return count, updates

def scheduled_inventory_sync():
    """Runs efficiently every 30 mins (Short lookback)"""
    with app.app_context():
        c, u = perform_inventory_sync(lookback_minutes=35) # 35 mins to cover overlapping
        if u > 0: # Only log if we actually did something to avoid spam
            log_event('Inventory', 'Success', f"Auto-Sync: Checked {c}, Updated {u}")

# --- ROUTES ---

@app.route('/')
def dashboard():
    # ... (Same dashboard logic) ...
    return render_template('dashboard.html', odoo_status=True if odoo else False, current_settings={}) # Simplified for brevity

@app.route('/live_logs')
def live_logs():
    return render_template('live_logs.html')

@app.route('/api/logs/live', methods=['GET'])
def api_live_logs():
    try:
        logs = SyncLog.query.order_by(SyncLog.timestamp.desc()).limit(100).all()
        data = []
        for log in logs:
            msg_type = 'info'
            status_lower = (log.status or '').lower()
            if 'error' in status_lower or 'fail' in status_lower: msg_type = 'error'
            elif 'success' in status_lower: msg_type = 'success'
            elif 'warning' in status_lower or 'skip' in status_lower: msg_type = 'warning'
            
            iso_ts = log.timestamp.isoformat()
            if not iso_ts.endswith('Z'): iso_ts += 'Z'
            data.append({'id': log.id, 'timestamp': iso_ts, 'message': f"[{log.entity}] {log.message}", 'type': msg_type, 'details': log.status})
        return jsonify(data)
    except: return jsonify([])

@app.route('/sync/inventory', methods=['GET'])
def sync_inventory_endpoint():
    """Manual Trigger (Force Sync - 365 Days)"""
    log_event('System', 'Info', 'Manual Trigger: Starting Inventory Sync (Full Scan)...')
    with app.app_context():
        c, u = perform_inventory_sync(lookback_minutes=525600) # 365 Days
        log_event('Inventory', 'Success', f"Manual Sync Complete. Checked {c}, Updated {u}")
        return jsonify({"synced": c, "updates": u})

# --- ROUTE FOR FIRST TIME CATEGORY SYNC ---
@app.route('/sync/categories/run_initial_import', methods=['GET'])
def run_initial_category_import():
    threading.Thread(target=sync_categories_only).start()
    return jsonify({"message": "Job Started: Syncing Categories from Shopify for products with empty Odoo categories."})

# --- PRODUCT WEBHOOKS ---
@app.route('/webhook/products/create', methods=['POST'])
@app.route('/webhook/products/update', methods=['POST'])
def product_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): 
        return "Unauthorized", 401
    
    with app.app_context():
        # process_product_data handles both creation and category updates
        process_product_data(request.json)
        
    return "Received", 200

# ... (Other routes: orders manual, import batch, webhooks, simulate etc. same as before) ...
@app.route('/sync/products/master', methods=['POST'])
def trigger_master_sync():
    threading.Thread(target=sync_products_master).start()
    return jsonify({"message": "Started"})

@app.route('/sync/customers/master', methods=['POST'])
def trigger_customer_master_sync():
    threading.Thread(target=sync_customers_master).start()
    return jsonify({"message": "Started"})

@app.route('/sync/products/archive_duplicates', methods=['POST'])
def trigger_duplicate_scan():
    threading.Thread(target=archive_shopify_duplicates).start()
    return jsonify({"message": "Started"})

@app.route('/sync/orders/manual', methods=['GET'])
def manual_order_fetch():
    # ... same implementation ...
    return jsonify({"orders": []}) 

@app.route('/sync/orders/import_batch', methods=['POST'])
def import_selected_orders():
    # ... same implementation ...
    return jsonify({"message": "Done"}) 

@app.route('/webhook/orders', methods=['POST'])
@app.route('/webhook/orders/updated', methods=['POST'])
def order_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    with app.app_context(): process_order_data(request.json)
    return "Received", 200

@app.route('/webhook/orders/cancelled', methods=['POST'])
def order_cancelled_webhook():
    return "Received", 200

@app.route('/webhook/refunds', methods=['POST'])
def refund_webhook():
    return "Received", 200

@app.route('/test/simulate_order', methods=['POST'])
def test_sim_dummy():
     log_event('System', 'Success', "Test Connection Triggered by User")
     return jsonify({"message": "OK"})

@app.route('/api/odoo/companies', methods=['GET'])
def api_get_companies():
    if odoo: return jsonify(odoo.get_companies())
    return jsonify([])

@app.route('/api/odoo/locations', methods=['GET'])
def api_get_locations():
    if odoo: return jsonify(odoo.get_locations(request.args.get('company_id')))
    return jsonify([])

@app.route('/api/settings/save', methods=['POST'])
def api_save_settings():
    # ... same saving logic ...
    return jsonify({"message": "Saved"})

def run_schedule():
    schedule.every(1).days.do(sync_products_master)
    schedule.every(1).days.do(sync_customers_master)
    schedule.every(30).days.do(archive_shopify_duplicates)
    
    # NEW: Inventory Sync every 30 minutes
    schedule.every(30).minutes.do(scheduled_inventory_sync)
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    t = threading.Thread(target=run_schedule, daemon=True)
    t.start()
    app.run(debug=True)
