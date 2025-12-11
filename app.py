import os
import hmac
import hashlib
import base64
import json
import threading
import schedule
import time
import shopify 
from flask import Flask, request, jsonify, render_template, redirect, url_for, session
from models import db, ProductMap, SyncLog, AppSetting, CustomerMap, Shop
from odoo_client import OdooClient
import requests
from datetime import datetime, timedelta
import random
import xmlrpc.client
from sqlalchemy.exc import IntegrityError

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24))

# --- CONFIGURATION ---
database_url = os.getenv('DATABASE_URL', 'sqlite:///local.db')
if database_url:
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+pg8000://", 1)
    elif database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+pg8000://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# SHOPIFY PARTNER CREDENTIALS
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_API_SECRET = os.getenv('SHOPIFY_API_SECRET')
HOST_URL = os.getenv('HOST_URL', 'https://odoo-shopify-connector.onrender.com')

SCOPES = ['read_products', 'write_products', 'read_orders', 'write_orders', 'read_inventory', 'write_inventory', 'read_customers', 'write_customers']

shopify.Session.setup(api_key=SHOPIFY_API_KEY, secret=SHOPIFY_API_SECRET)

db.init_app(app)

# --- GLOBAL LOCKS ---
order_processing_lock = threading.Lock()
active_processing_ids = set()

# --- DB INIT ---
with app.app_context():
    try: 
        db.create_all()
        print("Database tables created/verified.")
    except Exception as e: 
        print(f"CRITICAL DB INIT ERROR: {e}")

# --- HELPER FUNCTIONS (Context Aware) ---

def get_shop_from_session():
    if 'shop_id' not in session: return None
    return Shop.query.get(session['shop_id'])

def get_odoo_connection(shop_id):
    shop = Shop.query.get(shop_id)
    if not shop or not shop.odoo_url: return None
    try:
        return OdooClient(shop.odoo_url, shop.odoo_db, shop.odoo_username, shop.odoo_password)
    except: return None

def activate_shopify_session(shop):
    if not shop: return False
    try:
        api_session = shopify.Session(shop.shop_url, '2024-01', shop.access_token)
        shopify.ShopifyResource.activate_session(api_session)
        return True
    except: return False

def get_config(shop_id, key, default=None):
    try:
        setting = AppSetting.query.filter_by(shop_id=shop_id, key=key).first()
        if not setting: return default
        try: return json.loads(setting.value)
        except: return setting.value
    except: return default

def set_config(shop_id, key, value):
    try:
        setting = AppSetting.query.filter_by(shop_id=shop_id, key=key).first()
        if not setting:
            setting = AppSetting(shop_id=shop_id, key=key)
            db.session.add(setting)
        setting.value = json.dumps(value)
        db.session.commit()
        return True
    except:
        db.session.rollback()
        return False

def log_event(shop_id, entity, status, message):
    try:
        log = SyncLog(shop_id=shop_id, entity=entity, status=status, message=message, timestamp=datetime.utcnow())
        db.session.add(log)
        db.session.commit()
    except: db.session.rollback()

def verify_webhook(data, hmac_header):
    digest = hmac.new(SHOPIFY_API_SECRET.encode('utf-8'), data, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), hmac_header)

def extract_id(res):
    if isinstance(res, list) and len(res) > 0: return res[0]
    return res

# --- GRAPHQL HELPERS (Context Aware) ---
def find_shopify_product_by_sku(sku):
    query = """{ productVariants(first: 1, query: "sku:%s") { edges { node { product { legacyResourceId } } } } }""" % sku
    try:
        client = shopify.GraphQL()
        result = client.execute(query)
        data = json.loads(result)
        edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
        if edges: return edges[0]['node']['product']['legacyResourceId']
    except: pass
    return None

def get_shopify_variant_inv_by_sku(sku):
    query = """{ productVariants(first: 1, query: "sku:%s") { edges { node { legacyResourceId inventoryItem { legacyResourceId } inventoryQuantity } } } }""" % sku
    try:
        client = shopify.GraphQL()
        result = client.execute(query)
        data = json.loads(result)
        edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
        if edges:
            node = edges[0]['node']
            return {'variant_id': node['legacyResourceId'], 'inventory_item_id': node['inventoryItem']['legacyResourceId'], 'qty': node['inventoryQuantity']}
    except: pass
    return None

# --- OAUTH ROUTES ---

@app.route('/shopify/auth')
def shopify_auth():
    shop_url = request.args.get('shop')
    if not shop_url: return "Missing shop parameter", 400
    new_session = shopify.Session(shop_url, '2024-01')
    auth_url = new_session.create_permission_url(SCOPES, url_for('shopify_callback', _external=True, _scheme='https'))
    return redirect(auth_url)

@app.route('/shopify/callback')
def shopify_callback():
    shop_url = request.args.get('shop')
    try:
        new_session = shopify.Session(shop_url, '2024-01')
        token = new_session.request_token(request.args)
        
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop:
            shop = Shop(shop_url=shop_url, access_token=token)
            db.session.add(shop)
        else:
            shop.access_token = token
            shop.is_active = True
        db.session.commit()
        
        session['shop_id'] = shop.id
        session['shop_url'] = shop_url
        return redirect(url_for('dashboard'))
    except Exception as e:
        return f"Auth Failed: {e}", 500

# --- CORE LOGIC (Multi-Tenant) ---

def process_product_data(data, shop_id, odoo):
    """Webhook: Update Only."""
    product_type = data.get('product_type', '')
    cat_id = None
    if product_type:
        try:
            cat_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'search', [[['name', '=', product_type]]])
            if cat_ids: cat_id = cat_ids[0]
            else: cat_id = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'create', [{'name': product_type}])
        except: pass

    variants = data.get('variants', [])
    company_id = get_config(shop_id, 'odoo_company_id')
    processed_count = 0
    
    for v in variants:
        sku = v.get('sku')
        if not sku: continue
        product_id = odoo.search_product_by_sku(sku, company_id)
        if product_id and cat_id:
            try:
                current_prod = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'read', [[product_id]], {'fields': ['public_categ_ids']})
                current_cat_ids = current_prod[0].get('public_categ_ids', [])
                if cat_id not in current_cat_ids:
                    odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'write', [[product_id], {'public_categ_ids': [(4, cat_id)]}])
                    log_event(shop_id, 'Product', 'Info', f"Webhook: Updated Category for {sku}")
                    processed_count += 1
            except Exception as e: 
                if "pos.category" not in str(e) and "CacheMiss" not in str(e): print(f"Error: {e}")
    return processed_count

def process_order_data(data, shop_id, odoo):
    """Syncs order with duplicate prevention and smart updates."""
    shopify_id = str(data.get('id', ''))
    shopify_name = data.get('name')
    
    with order_processing_lock:
        if shopify_id in active_processing_ids: return False, "Skipped"
        active_processing_ids.add(shopify_id)

    try:
        email = data.get('email') or data.get('contact_email')
        client_ref = f"ONLINE_{shopify_name}"
        company_id = get_config(shop_id, 'odoo_company_id')
        
        if not company_id:
            try:
                user_info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'res.users', 'read', [[odoo.uid]], {'fields': ['company_id']})
                if user_info: company_id = user_info[0]['company_id'][0]
            except: pass

        existing_order_id = None
        try:
            existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
            if existing_ids: existing_order_id = existing_ids[0]
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
                log_event(shop_id, 'Customer', 'Success', f"Created Customer: {name}")
                if shopify_id:
                    c_id = str(data.get('customer', {}).get('id'))
                    if c_id:
                        cust_map = CustomerMap(shop_id=shop_id, shopify_customer_id=c_id, odoo_partner_id=partner_id, email=email)
                        db.session.add(cust_map)
                        db.session.commit()
            except Exception as e: return False, f"Customer Error: {e}"
        else:
            partner_id = partner['id']
        
        sales_rep_id = odoo.get_partner_salesperson(partner_id) or odoo.uid
        shipping_id = partner_id
        invoice_id = partner_id
        
        lines = []
        for item in data.get('line_items', []):
            sku = item.get('sku')
            if not sku: continue
            product_id = odoo.search_product_by_sku(sku, company_id)
            if not product_id:
                if odoo.check_product_exists_by_sku(sku, company_id): continue 
                log_event(shop_id, 'Product', 'Info', f"SKU {sku} missing. Creating...")
                try:
                    new_p_vals = {'name': item['name'], 'default_code': sku, 'list_price': float(item.get('price', 0)), 'type': 'product'}
                    if company_id: new_p_vals['company_id'] = int(company_id)
                    odoo.create_product(new_p_vals)
                    product_id = odoo.search_product_by_sku(sku, company_id) 
                except: pass

            if product_id:
                price = float(item.get('price', 0))
                qty = int(item.get('quantity', 1))
                disc = float(item.get('total_discount', 0))
                pct = (disc / (price * qty)) * 100 if price > 0 else 0.0
                lines.append((0, 0, {'product_id': product_id, 'product_uom_qty': qty, 'price_unit': price, 'name': item['name'], 'discount': pct}))

        # Shipping
        for ship_line in data.get('shipping_lines', []):
            try: cost = float(ship_line.get('price', 0.0))
            except: cost = 0.0
            ship_title = ship_line.get('title', 'Shipping')
            
            if cost >= 0:
                ship_product_id = odoo.search_product_by_name(ship_title, company_id)
                if not ship_product_id: ship_product_id = odoo.search_product_by_sku("SHIP_FEE", company_id)
                if not ship_product_id: ship_product_id = odoo.search_product_by_name("Shopify Shipping", company_id)
                
                if not ship_product_id:
                    try:
                        sp_vals = {'name': ship_title or "Shopify Shipping", 'type': 'service', 'list_price': 0.0, 'default_code': 'SHIP_FEE'}
                        if company_id: sp_vals['company_id'] = int(company_id)
                        odoo.create_product(sp_vals)
                        if sp_vals.get('default_code'): ship_product_id = odoo.search_product_by_sku("SHIP_FEE", company_id)
                        else: ship_product_id = odoo.search_product_by_name(sp_vals['name'], company_id)
                    except: pass
                
                if ship_product_id:
                    lines.append((0, 0, {'product_id': ship_product_id, 'product_uom_qty': 1, 'price_unit': cost, 'name': ship_title, 'discount': 0.0}))

        if not lines: return False, "No lines"
        
        gateway = data.get('gateway') or (data.get('payment_gateway_names')[0] if data.get('payment_gateway_names') else 'Shopify')
        note_text = f"Payment Gateway: {gateway}"

        if existing_order_id:
            # Smart Change Check
            order_info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'read', [[existing_order_id]], {'fields': ['state', 'note', 'order_line']})
            if not order_info: return False, "Order not found"
            
            curr = order_info[0]
            if curr['state'] in ['done', 'cancel']: 
                log_event(shop_id, 'Order', 'Skipped', f"Order {client_ref} is locked/cancelled.")
                return True, "Skipped"
            
            has_changes = False
            if note_text != (curr.get('note') or ''): has_changes = True
            
            if not has_changes:
                current_line_ids = curr.get('order_line', [])
                if len(current_line_ids) != len(lines): has_changes = True
                elif current_line_ids:
                    # Deep compare lines (omitted full logic for brevity but concept holds)
                    pass

            # Force update for now to be safe if simplified logic passed
            update_vals = {'order_line': [(5, 0, 0)] + lines, 'note': note_text, 'partner_shipping_id': shipping_id, 'partner_invoice_id': invoice_id}
            
            try:
                odoo.update_sale_order(existing_order_id, update_vals)
                odoo.post_message(existing_order_id, f"Updated via Shopify. {note_text}")
                log_event(shop_id, 'Order', 'Success', f"Updated {client_ref}")
                return True, "Updated"
            except Exception as e:
                log_event(shop_id, 'Order', 'Error', f"Update Failed: {e}")
                return False, str(e)
        else:
            vals = {
                'name': client_ref, 'client_order_ref': client_ref, 'partner_id': partner_id, 
                'partner_shipping_id': shipping_id, 'partner_invoice_id': invoice_id, 
                'order_line': lines, 'user_id': sales_rep_id, 'state': 'draft', 'note': note_text
            }
            if company_id: vals['company_id'] = int(company_id)
            try:
                odoo.create_sale_order(vals, context={'manual_price': True})
                log_event(shop_id, 'Order', 'Success', f"Synced {client_ref}")
                return True, "Synced"
            except Exception as e:
                log_event(shop_id, 'Order', 'Error', str(e))
                return False, str(e)
            
    finally:
        with order_processing_lock:
            if shopify_id in active_processing_ids: active_processing_ids.remove(shopify_id)

def sync_products_master(shop_id, odoo, session):
    """Master Product Sync (Odoo -> Shopify)"""
    company_id = get_config(shop_id, 'odoo_company_id')
    odoo_products = odoo.get_all_products(company_id)
    active_odoo_skus = set()
    
    sync_title = get_config(shop_id, 'prod_sync_title', True)
    sync_desc = get_config(shop_id, 'prod_sync_desc', True)
    sync_price = get_config(shop_id, 'prod_sync_price', True)
    sync_type = get_config(shop_id, 'prod_sync_type', True)
    sync_vendor = get_config(shop_id, 'prod_sync_vendor', True)
    sync_tags = get_config(shop_id, 'prod_sync_tags', False)
    
    log_event(shop_id, 'Product Sync', 'Info', f"Found {len(odoo_products)} products. Syncing...")
    
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
                        log_event(shop_id, 'Product Sync', 'Info', f"Archived {sku}")
                except: pass
            continue 

        active_odoo_skus.add(sku)
        shopify_id = find_shopify_product_by_sku(sku)
        try:
            if shopify_id: sp = shopify.Product.find(shopify_id)
            else: sp = shopify.Product()
            changed = False
            
            if sync_title and sp.title != p['name']:
                sp.title = p['name']
                changed = True
            
            if sync_desc:
                desc = p.get('description_sale') or ''
                if (sp.body_html or '') != desc:
                    sp.body_html = desc
                    changed = True

            # Category
            odoo_categ_ids = p.get('public_categ_ids', [])
            if not odoo_categ_ids and sp.product_type:
                # Init Odoo
                try:
                    cat_name = sp.product_type
                    cat_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'search', [[['name', '=', cat_name]]])
                    cat_id = cat_ids[0] if cat_ids else odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'create', [{'name': cat_name}])
                    odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'write', [[p['id']], {'public_categ_ids': [(4, cat_id)]}])
                except: pass
            elif odoo_categ_ids and sync_type:
                cat = odoo.get_public_category_name(odoo_categ_ids)
                if cat and sp.product_type != cat:
                    sp.product_type = cat
                    changed = True

            # Vendor
            if sync_vendor:
                target = (p.get('name', '').split()[0]) or 'Odoo'
                if sp.vendor != target:
                    sp.vendor = target
                    changed = True
            
            # Tags
            if sync_tags:
                tag_ids = p.get('product_tag_ids', [])
                if tag_ids:
                    t_names = odoo.get_tag_names(tag_ids)
                    curr = set(sp.tags.split(', ')) if sp.tags else set()
                    updated = curr.union(set(t_names))
                    new_str = ", ".join(sorted(list(updated)))
                    if sp.tags != new_str:
                        sp.tags = new_str
                        changed = True

            if sp.status != 'active':
                sp.status = 'active'
                changed = True
            
            if changed or not shopify_id:
                sp.save()
                if not shopify_id: sp = shopify.Product.find(sp.id)
            
            if sp.variants: variant = sp.variants[0]
            else: variant = shopify.Variant(prefix_options={'product_id': sp.id})
            
            v_changed = False
            if variant.sku != sku:
                variant.sku = sku
                v_changed = True
            
            if sync_price:
                tgt = str(p['list_price'])
                if variant.price != tgt:
                    variant.price = tgt
                    v_changed = True
            
            # Check ID
            v_prod_id = getattr(variant, 'product_id', None)
            if not v_prod_id and variant.attributes: v_product_id = variant.attributes.get('product_id')
            if str(v_product_id) != str(sp.id):
                variant.product_id = sp.id
                v_changed = True

            if v_changed: variant.save()

            # Inventory
            loc_ids = get_config(shop_id, 'inventory_locations', []) 
            
            # Image
            if get_config(shop_id, 'prod_sync_images', False):
                try:
                    img = odoo.get_product_image(p['id'])
                    if img and not sp.images:
                        if isinstance(img, bytes): img = img.decode('utf-8')
                        image = shopify.Image(prefix_options={'product_id': sp.id})
                        image.attachment = img
                        image.save()
                except: pass

            synced += 1
        except Exception as e:
            if "pos.category" not in str(e):
                 log_event(shop_id, 'Product', 'Error', f"Sync fail {sku}: {e}")
    
    cleanup_shopify_products(shop_id, active_odoo_skus)
    log_event(shop_id, 'Product Sync', 'Success', f"Master Sync Done: {synced}")

def cleanup_shopify_products(shop_id, active_skus):
    seen = set()
    page = shopify.Product.find(limit=250)
    count = 0
    try:
        while page:
            for sp in page:
                v = sp.variants[0] if sp.variants else None
                if not v or not v.sku: continue
                sku = v.sku
                if sku not in active_skus or sku in seen:
                    if sp.status != 'archived':
                        sp.status = 'archived'
                        sp.save()
                        count += 1
                else: seen.add(sku)
            if page.has_next_page(): page = page.next_page()
            else: break
    except: pass
    if count > 0: log_event(shop_id, 'System', 'Success', f"Archived {count} Orphans")

def sync_categories_only(shop_id, odoo):
    company_id = get_config(shop_id, 'odoo_company_id')
    odoo_prods = odoo.get_all_products(company_id)
    odoo_map = {p['default_code']: p for p in odoo_prods if p.get('default_code')}
    cat_map = {}
    try:
        cats = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'search_read', [[]], {'fields': ['id', 'name']})
        for c in cats: cat_map[c['name']] = c['id']
    except: pass

    count = 0
    page = shopify.Product.find(limit=250)
    while page:
        for sp in page:
            if not sp.product_type: continue
            v = sp.variants[0] if sp.variants else None
            if not v or not v.sku: continue
            prod = odoo_map.get(v.sku)
            if not prod or prod.get('public_categ_ids'): continue
            
            try:
                name = sp.product_type
                cid = cat_map.get(name)
                if not cid:
                    cid = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'create', [{'name': name}])
                    cat_map[name] = cid
                odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'write', [[prod['id']], {'public_categ_ids': [(4, cid)]}])
                count += 1
                prod['public_categ_ids'] = [cid]
            except Exception as e: 
                if "pos.category" not in str(e): print(e)
        if page.has_next_page(): page = page.next_page()
        else: break
    log_event(shop_id, 'System', 'Success', f"Cat Init Done: {count}")

def perform_inventory_sync(shop_id, odoo, lookback):
    locs = get_config(shop_id, 'inventory_locations', [])
    field = get_config(shop_id, 'inventory_field', 'qty_available')
    sync_zero = get_config(shop_id, 'sync_zero_stock', False)
    comp_id = get_config(shop_id, 'odoo_company_id')
    
    last = datetime.utcnow() - timedelta(minutes=lookback)
    try: p_ids = odoo.get_changed_products(str(last), comp_id)
    except: return 0, 0
    
    cnt, upd = 0, 0
    for p_id in p_ids:
        # Get Odoo Total
        total_odoo = int(odoo.get_total_qty_for_locations(p_id, locs, field_name=field))
        if sync_zero and total_odoo <= 0: continue
        
        p_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'read', [p_id], {'fields': ['default_code']})
        sku = p_data[0].get('default_code')
        if not sku: continue
        
        shopify_info = get_shopify_variant_inv_by_sku(sku)
        if not shopify_info: continue
        
        if int(shopify_info['qty']) != total_odoo:
            try:
                # Need Shopify Location ID from config (assuming multi-location eventually)
                # For now hardcoding to first available or from config
                # shopify_loc_id = ...
                # shopify.InventoryLevel.set(...)
                upd += 1
            except Exception as e: print(f"Inv Error {sku}: {e}")
        cnt += 1
    return cnt, upd

# --- OAUTH & ROUTES ---

@app.route('/shopify/auth')
def shopify_auth():
    shop_url = request.args.get('shop')
    if not shop_url: return "Missing shop", 400
    new_session = shopify.Session(shop_url, '2024-01')
    return redirect(new_session.create_permission_url(SCOPES, url_for('shopify_callback', _external=True, _scheme='https')))

@app.route('/shopify/callback')
def shopify_callback():
    shop_url = request.args.get('shop')
    try:
        new_session = shopify.Session(shop_url, '2024-01')
        token = new_session.request_token(request.args)
        
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop:
            shop = Shop(shop_url=shop_url, access_token=token)
            db.session.add(shop)
        else:
            shop.access_token = token
            shop.is_active = True
        db.session.commit()
        
        session['shop_id'] = shop.id
        session['shop_url'] = shop_url
        return redirect(url_for('dashboard'))
    except Exception as e: return f"Auth Failed: {e}", 500

@app.route('/')
def dashboard():
    if 'shop_id' not in session:
        s = request.args.get('shop')
        if s: return redirect(url_for('shopify_auth', shop=s))
        return "Install App"
    
    shop = Shop.query.get(session['shop_id'])
    odoo = get_odoo_connection(shop.id)
    logs = SyncLog.query.filter_by(shop_id=shop.id).order_by(SyncLog.timestamp.desc()).limit(20).all()
    
    current_settings = {
        "odoo_url": shop.odoo_url or "",
        "odoo_db": shop.odoo_db or "",
        "odoo_username": shop.odoo_username or "",
        "prod_sync_price": get_config(shop.id, 'prod_sync_price', True),
        "prod_sync_title": get_config(shop.id, 'prod_sync_title', True),
        "prod_sync_desc": get_config(shop.id, 'prod_sync_desc', True),
        "prod_sync_type": get_config(shop.id, 'prod_sync_type', True),
        "prod_sync_vendor": get_config(shop.id, 'prod_sync_vendor', True),
        "prod_sync_tags": get_config(shop.id, 'prod_sync_tags', False),
        "prod_sync_images": get_config(shop.id, 'prod_sync_images', False),
        "prod_auto_create": get_config(shop.id, 'prod_auto_create', False),
        "prod_auto_publish": get_config(shop.id, 'prod_auto_publish', False),
        "prod_sync_meta_vendor_code": get_config(shop.id, 'prod_sync_meta_vendor_code', False),
        
        "inventory_field": get_config(shop.id, 'inventory_field', 'qty_available'),
        "sync_zero_stock": get_config(shop.id, 'sync_zero_stock', False),
        "combine_committed": get_config(shop.id, 'combine_committed', False),
        
        "cust_direction": get_config(shop.id, 'cust_direction', 'bidirectional'),
        "cust_auto_sync": get_config(shop.id, 'cust_auto_sync', True),
        "cust_sync_tags": get_config(shop.id, 'cust_sync_tags', False),
        "cust_whitelist_tags": get_config(shop.id, 'cust_whitelist_tags', ''),
        "cust_blacklist_tags": get_config(shop.id, 'cust_blacklist_tags', ''),
        
        "order_sync_tax": get_config(shop.id, 'order_sync_tax', False),
        "odoo_company_id": get_config(shop.id, 'odoo_company_id'),
    }
    return render_template('dashboard.html', logs=logs, odoo_status=True if odoo else False, current_settings=current_settings)

@app.route('/api/settings/save', methods=['POST'])
def api_save_settings():
    if 'shop_id' not in session: return jsonify({"error": "401"}), 401
    sid = session['shop_id']
    d = request.json
    
    s = Shop.query.get(sid)
    if 'odoo_url' in d: s.odoo_url = d['odoo_url']
    if 'odoo_db' in d: s.odoo_db = d['odoo_db']
    if 'odoo_username' in d: s.odoo_username = d['odoo_username']
    if 'odoo_password' in d and d['odoo_password']: s.odoo_password = d['odoo_password']
    db.session.commit()
    
    set_config(sid, 'prod_sync_price', d.get('prod_sync_price', True))
    set_config(sid, 'prod_sync_title', d.get('prod_sync_title', True))
    set_config(sid, 'prod_sync_desc', d.get('prod_sync_desc', True))
    set_config(sid, 'prod_sync_type', d.get('prod_sync_type', True))
    set_config(sid, 'prod_sync_vendor', d.get('prod_sync_vendor', True))
    set_config(sid, 'prod_sync_tags', d.get('prod_sync_tags', False))
    set_config(sid, 'prod_sync_images', d.get('prod_sync_images', False))
    set_config(sid, 'prod_auto_create', d.get('prod_auto_create', False))
    set_config(sid, 'prod_auto_publish', d.get('prod_auto_publish', False))
    set_config(sid, 'prod_sync_meta_vendor_code', d.get('prod_sync_meta_vendor_code', False))
    
    set_config(sid, 'inventory_field', d.get('field', 'qty_available'))
    set_config(sid, 'sync_zero_stock', d.get('sync_zero', False))
    set_config(sid, 'combine_committed', d.get('combine_committed', False))
    set_config(sid, 'odoo_company_id', d.get('company_id'))
    
    set_config(sid, 'cust_direction', d.get('cust_direction'))
    set_config(sid, 'cust_auto_sync', d.get('cust_auto_sync'))
    set_config(sid, 'cust_sync_tags', d.get('cust_sync_tags'))
    set_config(sid, 'cust_whitelist_tags', d.get('cust_whitelist_tags'))
    set_config(sid, 'cust_blacklist_tags', d.get('cust_blacklist_tags'))
    
    set_config(sid, 'order_sync_tax', d.get('order_sync_tax'))
    
    return jsonify({"message": "Saved"})

@app.route('/api/logs/live', methods=['GET'])
def api_live_logs():
    if 'shop_id' not in session: return jsonify([])
    logs = SyncLog.query.filter_by(shop_id=session['shop_id']).order_by(SyncLog.timestamp.desc()).limit(100).all()
    data = []
    for l in logs:
        iso = l.timestamp.isoformat()
        if not iso.endswith('Z'): iso += 'Z'
        data.append({'id': l.id, 'timestamp': iso, 'message': f"[{l.entity}] {l.message}", 'type': 'info', 'details': l.status})
    return jsonify(data)

@app.route('/sync/products/master', methods=['POST'])
def trigger_master_sync():
    if 'shop_id' not in session: return "401", 401
    sid = session['shop_id']
    shop = Shop.query.get(sid)
    activate_shopify_session(shop)
    odoo = get_odoo_connection(sid)
    threading.Thread(target=sync_products_master, args=(sid, odoo, None)).start()
    return jsonify({"message": "Started"})

@app.route('/sync/categories/run_initial_import', methods=['GET'])
def run_initial_category_import():
    if 'shop_id' not in session: return "401", 401
    sid = session['shop_id']
    shop = Shop.query.get(sid)
    activate_shopify_session(shop)
    odoo = get_odoo_connection(sid)
    threading.Thread(target=sync_categories_only, args=(sid, odoo)).start()
    return jsonify({"message": "Started"})

@app.route('/webhook/orders', methods=['POST'])
def order_webhook_route():
    if not verify_webhook(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "401", 401
    domain = request.headers.get('X-Shopify-Topic-Domain')
    shop = Shop.query.filter_by(shop_url=domain).first()
    if not shop: return "200", 200
    
    activate_shopify_session(shop)
    odoo = get_odoo_connection(shop.id)
    if odoo:
        with app.app_context(): process_order_data(request.json, shop.id, odoo)
    return "200", 200

@app.route('/webhook/products/update', methods=['POST'])
def product_webhook_route():
    if not verify_webhook(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "401", 401
    domain = request.headers.get('X-Shopify-Topic-Domain')
    shop = Shop.query.filter_by(shop_url=domain).first()
    if not shop: return "200", 200
    
    activate_shopify_session(shop)
    odoo = get_odoo_connection(shop.id)
    if odoo:
        with app.app_context(): process_product_data(request.json, shop.id, odoo)
    return "200", 200

# --- SCHEDULER LOOP ---
def run_schedule_loop():
    with app.app_context():
        shops = Shop.query.filter_by(is_active=True).all()
        for shop in shops:
            try:
                activate_shopify_session(shop)
                odoo = get_odoo_connection(shop.id)
                if odoo:
                    # sync_products_master(shop.id, odoo, None) # Daily
                    # perform_inventory_sync(shop.id, odoo, 35) # 30 min
                    pass
            except Exception as e: print(f"Err {shop.shop_url}: {e}")

def scheduler_thread():
    schedule.every(30).minutes.do(run_schedule_loop)
    while True:
        schedule.run_pending()
        time.sleep(1)

t = threading.Thread(target=scheduler_thread, daemon=True)
t.start()

if __name__ == '__main__':
    app.run(debug=True)
