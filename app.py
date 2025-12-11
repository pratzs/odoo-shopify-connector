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
import xmlrpc.client

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
order_processing_lock = threading.Lock()
active_processing_ids = set()

# --- HELPERS ---
def get_config(key, default=None):
    """Safely retrieve config with session management"""
    try:
        setting = AppSetting.query.get(key)
        if not setting:
            return default
        try: 
            return json.loads(setting.value)
        except: 
            return setting.value
    except Exception as e:
        print(f"Config Read Error ({key}): {e}")
        return default

def set_config(key, value):
    """Safely save config with rollback support"""
    try:
        setting = AppSetting.query.get(key)
        if not setting:
            setting = AppSetting(key=key)
            db.session.add(setting)
        setting.value = json.dumps(value)
        db.session.commit()
        return True
    except Exception as e:
        print(f"Config Save Error ({key}): {e}")
        db.session.rollback()
        return False

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
    """
    product_type = data.get('product_type', '')
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

    variants = data.get('variants', [])
    processed_count = 0
    company_id = get_config('odoo_company_id')
    
    for v in variants:
        sku = v.get('sku')
        if not sku: continue
        product_id = odoo.search_product_by_sku(sku, company_id)
        
        if product_id:
            if cat_id:
                try:
                    current_prod = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                        'product.product', 'read', [[product_id]], {'fields': ['public_categ_ids']})
                    current_cat_ids = current_prod[0].get('public_categ_ids', [])
                    if cat_id not in current_cat_ids:
                        odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                            'product.product', 'write', [[product_id], {'public_categ_ids': [(4, cat_id)]}])
                        log_event('Product', 'Info', f"Webhook: Updated Category for {sku} to '{product_type}'")
                        processed_count += 1
                except Exception as e:
                    err_msg = str(e)
                    if "pos.category" in err_msg or "CacheMiss" in err_msg or "KeyError" in err_msg:
                        pass
                    else:
                        print(f"Webhook Update Error: {e}")
    return processed_count

def process_order_data(data):
    """Syncs order. UPDATES existing orders instead of skipping."""
    shopify_id = str(data.get('id', ''))
    shopify_name = data.get('name')
    
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

        existing_order_id = None
        try:
            existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
            if existing_ids:
                existing_order_id = existing_ids[0]
        except Exception as e: return False, f"Odoo Error: {str(e)}"

        partner = odoo.search_partner_by_email(email)
        
        if not partner:
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
        sales_rep_id = odoo.get_partner_salesperson(partner_id)
        if not sales_rep_id: sales_rep_id = odoo.uid
        shipping_id = partner_id 
        invoice_id = partner_id 

        lines = []
        for item in data.get('line_items', []):
            sku = item.get('sku')
            if not sku: continue
            product_id = odoo.search_product_by_sku(sku, company_id)
            if not product_id:
                archived_id = odoo.check_product_exists_by_sku(sku, company_id)
                if archived_id:
                    log_event('Order', 'Warning', f"Skipped SKU {sku}: Product is Archived.")
                    continue 
                log_event('Product', 'Info', f"SKU {sku} missing on Order. Creating...")
                try:
                    new_p_vals = {
                        'name': item['name'], 'default_code': sku, 'list_price': float(item.get('price', 0)), 'type': 'product'
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
                lines.append((0, 0, {'product_id': product_id, 'product_uom_qty': qty, 'price_unit': price, 'name': item['name'], 'discount': pct}))
            else:
                log_event('Order', 'Warning', f"Skipped line {sku}: Product not found/created.")

        # --- SHIPPING LOGIC (FIXED) ---
        for ship_line in data.get('shipping_lines', []):
            try:
                cost = float(ship_line.get('price', 0.0))
            except: cost = 0.0
            
            ship_title = ship_line.get('title', 'Shipping')
            
            # Allow >= 0 to include Free Shipping
            if cost >= 0:
                ship_product_id = None
                
                # 1. First Priority: Search by exact Shipping Method Name (e.g. "Free Mobil Nationwide Shipping")
                if ship_title:
                    ship_product_id = odoo.search_product_by_name(ship_title, company_id)

                # 2. Second Priority: Search by Generic SKU 'SHIP_FEE'
                if not ship_product_id:
                    ship_product_id = odoo.search_product_by_sku("SHIP_FEE", company_id)
                
                # 3. Third Priority: Search by Generic Name
                if not ship_product_id:
                    ship_product_id = odoo.search_product_by_name("Shopify Shipping", company_id)
                
                if not ship_product_id:
                    log_event('Product', 'Info', f"Creating new Shipping Service: {ship_title}")
                    try:
                        sp_vals = {
                            'name': ship_title if ship_title else "Shopify Shipping", 
                            'type': 'service', 
                            'list_price': 0.0, 
                            'default_code': 'SHIP_FEE' if not odoo.search_product_by_sku("SHIP_FEE", company_id) else None 
                        }
                        if company_id: sp_vals['company_id'] = int(company_id)
                        odoo.create_product(sp_vals)
                        # Re-fetch based on what we just created
                        if sp_vals.get('default_code'):
                             ship_product_id = odoo.search_product_by_sku("SHIP_FEE", company_id)
                        else:
                             ship_product_id = odoo.search_product_by_name(sp_vals['name'], company_id)

                    except Exception as e:
                        log_event('Product', 'Error', f"Failed to create Shipping Product: {e}")

                if ship_product_id:
                    lines.append((0, 0, {
                        'product_id': ship_product_id,
                        'product_uom_qty': 1,
                        'price_unit': cost,
                        'name': ship_title,
                        'discount': 0.0
                    }))
                else:
                    log_event('Order', 'Warning', "Shipping line skipped: Could not find/create valid shipping product.")

        if not lines: return False, "No valid lines"
        
        # --- PAYMENT METHOD LOGIC (FIXED) ---
        gateway = data.get('gateway') or (data.get('payment_gateway_names')[0] if data.get('payment_gateway_names') else 'Shopify')
        note_text = f"Payment Gateway: {gateway}"

        if existing_order_id:
            order_info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'read', [[existing_order_id]], {'fields': ['state']})
            state = order_info[0]['state'] if order_info else 'unknown'
            if state in ['done', 'cancel']:
                log_event('Order', 'Skipped', f"Order {client_ref} is {state}. Update skipped.")
                return True, "Skipped"
            
            update_vals = {
                'order_line': [(5, 0, 0)] + lines,
                'partner_shipping_id': shipping_id,
                'partner_invoice_id': invoice_id,
                'note': note_text  # Update Note on edit
            }
            try:
                odoo.update_sale_order(existing_order_id, update_vals)
                odoo.post_message(existing_order_id, f"Order Updated via Shopify Sync. {note_text}")
                log_event('Order', 'Success', f"Updated {client_ref} (Revision)")
                return True, "Updated"
            except Exception as e:
                log_event('Order', 'Error', f"Update Failed: {e}")
                return False, str(e)
        else:
            vals = {
                'name': client_ref, 
                'client_order_ref': client_ref, 
                'partner_id': partner_id, 
                'partner_invoice_id': invoice_id, 
                'partner_shipping_id': shipping_id, 
                'order_line': lines, 
                'user_id': sales_rep_id, 
                'state': 'draft',
                'note': note_text  # Set Note on create
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
        with order_processing_lock:
            if shopify_id in active_processing_ids:
                active_processing_ids.remove(shopify_id)

def sync_products_master():
    """Odoo -> Shopify Product Sync"""
    with app.app_context():
        if not odoo or not setup_shopify_session(): 
            log_event('System', 'Error', "Product Sync Failed: Connection Error")
            return

        company_id = get_config('odoo_company_id')
        odoo_products = odoo.get_all_products(company_id)
        active_odoo_skus = set()
        
        log_event('Product Sync', 'Info', f"Found {len(odoo_products)} products. Starting Master Sync...")
        
        synced = 0
        for p in odoo_products:
            sku = p.get('default_code')
            if not sku: continue

            if not p.get('active', True):
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

            active_odoo_skus.add(sku)
            shopify_id = find_shopify_product_by_sku(sku)
            try:
                if shopify_id: sp = shopify.Product.find(shopify_id)
                else: sp = shopify.Product()
                product_changed = False
                
                if sp.title != p['name']:
                    sp.title = p['name']
                    product_changed = True
                
                odoo_desc = p.get('description_sale') or ''
                if (sp.body_html or '') != odoo_desc:
                    sp.body_html = odoo_desc
                    product_changed = True
                
                # Category Mapping
                odoo_categ_ids = p.get('public_categ_ids', [])
                if not odoo_categ_ids and sp.product_type:
                    try:
                        cat_name = sp.product_type
                        cat_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'search', [[['name', '=', cat_name]]])
                        cat_id = cat_ids[0] if cat_ids else None
                        if not cat_id:
                            cat_id = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'create', [{'name': cat_name}])
                        odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'write', [[p['id']], {'public_categ_ids': [(4, cat_id)]}])
                        log_event('Product Sync', 'Info', f"Initialized Odoo Category for {sku}: {cat_name}")
                    except Exception as e:
                        err_msg = str(e)
                        if "pos.category" in err_msg or "CacheMiss" in err_msg or "KeyError" in err_msg:
                             pass # Suppress Odoo POS crash errors
                        else:
                             print(f"Category Import Error: {e}")

                elif odoo_categ_ids:
                    odoo_cat_name = odoo.get_public_category_name(odoo_categ_ids)
                    if odoo_cat_name and sp.product_type != odoo_cat_name:
                        sp.product_type = odoo_cat_name
                        product_changed = True

                # Vendor Mapping (First word of Title)
                product_title = p.get('name', '')
                target_vendor = product_title.split()[0] if product_title else 'Odoo Master'
                if sp.vendor != target_vendor:
                    sp.vendor = target_vendor
                    product_changed = True

                if sp.status != 'active':
                    sp.status = 'active'
                    product_changed = True
                
                if product_changed or not shopify_id:
                    sp.save()
                    # RELOAD TO FIX KEY ERROR
                    if not shopify_id:
                        sp = shopify.Product.find(sp.id)
                
                if sp.variants: 
                    variant = sp.variants[0]
                else: 
                    variant = shopify.Variant(prefix_options={'product_id': sp.id})
                
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
                
                # Safely Check Product ID
                v_product_id = getattr(variant, 'product_id', None)
                if not v_product_id: 
                    if variant.attributes: v_product_id = variant.attributes.get('product_id')
                
                if str(v_product_id) != str(sp.id):
                    variant.product_id = sp.id
                    variant_changed = True

                if variant_changed: variant.save()
                
                if SHOPIFY_LOCATION_ID and variant.inventory_item_id:
                    # --- INVENTORY SYNC OPTIMIZATION ---
                    # Only update if different
                    qty = int(p.get('qty_available', 0))
                    try:
                         # Get current Shopify level to compare
                         current_inv = get_shopify_variant_inv_by_sku(sku)
                         if current_inv and int(current_inv['qty']) != qty:
                             shopify.InventoryLevel.set(location_id=SHOPIFY_LOCATION_ID, inventory_item_id=variant.inventory_item_id, available=qty)
                             log_event('Product Sync', 'Info', f"Updated Stock for {sku} during master sync: -> {qty}")
                    except: pass

                # --- NEW COST PRICE SYNC ---
                if variant.inventory_item_id:
                    try:
                        cost = float(p.get('standard_price', 0.0))
                        inv_item = shopify.InventoryItem.find(variant.inventory_item_id)
                        if float(inv_item.cost or 0) != cost:
                            inv_item.cost = cost
                            inv_item.save()
                            # log_event('Product Sync', 'Info', f"Updated Cost for {sku}") 
                    except Exception as cost_e:
                        print(f"Cost Sync Error {sku}: {cost_e}")

                # Image Sync Logic with Isolation
                try:
                    img_data = odoo.get_product_image(p['id'])
                    if img_data and not sp.images:
                        if isinstance(img_data, bytes):
                            img_data = img_data.decode('utf-8')
                            
                        image = shopify.Image(prefix_options={'product_id': sp.id})
                        image.attachment = img_data
                        image.save()
                        log_event('Product Sync', 'Info', f"Synced Image for {sku}")
                except Exception as img_e:
                     log_event('Product Sync', 'Warning', f"Image Sync Failed for {sku}: {img_e}")

                vendor_code = odoo.get_vendor_product_code(p['product_tmpl_id'][0])
                if vendor_code:
                    metafield = shopify.Metafield({
                        'key': 'vendor_product_code', 'value': vendor_code, 'type': 'single_line_text_field',
                        'namespace': 'custom', 'owner_resource': 'product', 'owner_id': sp.id
                    })
                    metafield.save()
                synced += 1
            except Exception as e:
                err_msg = str(e)
                if "pos.category" in err_msg or "CacheMiss" in err_msg:
                    pass 
                else:
                    log_event('Product Sync', 'Error', f"Failed {sku}: {e}")
        
        cleanup_shopify_products(active_odoo_skus)
        log_event('Product Sync', 'Success', f"Master Sync Complete. Processed {synced} active products.")

def sync_categories_only():
    """Optimized ONE-TIME import of Categories from Shopify to Odoo."""
    with app.app_context():
        if not odoo or not setup_shopify_session(): 
            log_event('System', 'Error', "Category Sync Failed: Connection Error")
            return

        log_event('System', 'Info', "Starting Optimized Category Sync...")
        company_id = get_config('odoo_company_id')
        odoo_prods = odoo.get_all_products(company_id)
        odoo_map = {p['default_code']: p for p in odoo_prods if p.get('default_code')}
        
        cat_map = {}
        try:
            cats = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'search_read', [[]], {'fields': ['id', 'name']})
            for c in cats: cat_map[c['name']] = c['id']
        except Exception as e: print(f"Cache Error: {e}")

        updated_count = 0
        page = shopify.Product.find(limit=250)
        while page:
            for sp in page:
                if not sp.product_type: continue
                variant = sp.variants[0] if sp.variants else None
                if not variant or not variant.sku: continue
                sku = variant.sku
                
                odoo_prod = odoo_map.get(sku)
                if not odoo_prod or odoo_prod.get('public_categ_ids') or not odoo_prod.get('active', True): continue

                try:
                    cat_name = sp.product_type
                    cat_id = cat_map.get(cat_name)
                    if not cat_id:
                        cat_id = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'create', [{'name': cat_name}])
                        cat_map[cat_name] = cat_id
                    
                    odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'write', [[odoo_prod['id']], {'public_categ_ids': [(4, cat_id)]}])
                    updated_count += 1
                    odoo_prod['public_categ_ids'] = [cat_id] 
                except Exception as e:
                    err_msg = str(e)
                    if "pos.category" in err_msg or "CacheMiss" in err_msg:
                        pass 
                    else:
                        print(f"Error syncing category for {sku}: {e}")

            if page.has_next_page(): page = page.next_page()
            else: break
        
        log_event('System', 'Success', f"Category Sync Finished. Updated {updated_count} products.")

def cleanup_shopify_products(odoo_active_skus):
    if not setup_shopify_session(): return
    seen_skus = set()
    products = shopify.Product.find(limit=250)
    page = products
    archived_count = 0
    try:
        while page:
            for sp in page:
                variant = sp.variants[0] if sp.variants else None
                if not variant or not variant.sku: continue
                sku = variant.sku
                needs_archive = False
                if sku not in odoo_active_skus: needs_archive = True
                elif sku in seen_skus: needs_archive = True
                
                if needs_archive:
                    if sp.status != 'archived':
                        sp.status = 'archived'
                        sp.save()
                        archived_count += 1
                else: seen_skus.add(sku)
            if page.has_next_page(): page = page.next_page()
            else: break
    except: pass
    if archived_count > 0: log_event('System', 'Success', f"Cleanup Complete. Archived {archived_count} products.")

def perform_inventory_sync(lookback_minutes):
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
    try: product_ids = odoo.get_changed_products(str(last_run), company_id)
    except: return 0, 0
    
    count = 0
    updates = 0
    for p_id in product_ids:
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
            except Exception as e: print(f"Inv Error {sku}: {e}")
        count += 1
    return count, updates

def scheduled_inventory_sync():
    with app.app_context():
        c, u = perform_inventory_sync(lookback_minutes=35)
        if u > 0: log_event('Inventory', 'Success', f"Auto-Sync: Checked {c}, Updated {u}")

@app.route('/')
def dashboard():
    return render_template('dashboard.html', odoo_status=True if odoo else False, current_settings={}) 

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
    log_event('System', 'Info', 'Manual Trigger: Starting Inventory Sync (Full Scan)...')
    with app.app_context():
        c, u = perform_inventory_sync(lookback_minutes=525600)
        log_event('Inventory', 'Success', f"Manual Sync Complete. Checked {c}, Updated {u}")
        return jsonify({"synced": c, "updates": u})

@app.route('/sync/categories/run_initial_import', methods=['GET'])
def run_initial_category_import():
    threading.Thread(target=sync_categories_only).start()
    return jsonify({"message": "Job Started"})

@app.route('/webhook/products/create', methods=['POST'])
@app.route('/webhook/products/update', methods=['POST'])
def product_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    with app.app_context(): process_product_data(request.json)
    return "Received", 200

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
def manual_order_fetch(): return jsonify({"orders": []})

@app.route('/sync/orders/import_batch', methods=['POST'])
def import_selected_orders(): return jsonify({"message": "Done"})

@app.route('/webhook/orders', methods=['POST'])
@app.route('/webhook/orders/updated', methods=['POST'])
def order_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    with app.app_context(): process_order_data(request.json)
    return "Received", 200

@app.route('/webhook/orders/cancelled', methods=['POST'])
def order_cancelled_webhook(): return "Received", 200

@app.route('/webhook/refunds', methods=['POST'])
def refund_webhook(): return "Received", 200

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
    data = request.json
    if set_config('inventory_locations', data.get('locations', [])):
        set_config('inventory_field', data.get('field', 'qty_available'))
        set_config('sync_zero_stock', data.get('sync_zero', False))
        set_config('combine_committed', data.get('combine_committed', False))
        set_config('odoo_company_id', data.get('company_id'))
        set_config('cust_direction', data.get('cust_direction'))
        set_config('cust_auto_sync', data.get('cust_auto_sync'))
        set_config('cust_sync_tags', data.get('cust_sync_tags'))
        set_config('cust_whitelist_tags', data.get('cust_whitelist_tags', ''))
        set_config('cust_blacklist_tags', data.get('cust_blacklist_tags', ''))
        return jsonify({"message": "Saved"})
    else:
        return jsonify({"message": "Error Saving"}), 500

def run_schedule():
    schedule.every(1).days.do(sync_products_master)
    schedule.every(1).days.do(sync_customers_master)
    schedule.every(30).days.do(archive_shopify_duplicates)
    schedule.every(30).minutes.do(scheduled_inventory_sync)
    while True:
        schedule.run_pending()
        time.sleep(1)

# --- START SCHEDULER (Threaded, outside main so Gunicorn sees it) ---
t = threading.Thread(target=run_schedule, daemon=True)
t.start()

if __name__ == '__main__':
    # Flask Dev Server
    app.run(debug=True)
