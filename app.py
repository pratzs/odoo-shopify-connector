import os
import hmac
import hashlib
import base64
import json
from flask import Flask, request, jsonify, render_template
from models import db, ProductMap, SyncLog, AppSetting
from odoo_client import OdooClient
import requests
from datetime import datetime, timedelta
import random
import threading
import time

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
    """Helper to extract ID from Odoo response if it's a list"""
    if isinstance(res, list) and len(res) > 0:
        return res[0]
    return res

def get_odoo_address_codes(country_id, state_id):
    """Fetch ISO codes for Country and State from Odoo IDs"""
    c_code = None
    s_code = None
    try:
        if country_id:
            c_res = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'res.country', 'read', [extract_id(country_id)], {'fields': ['code']})
            if c_res: c_code = c_res[0].get('code')
        if state_id:
            s_res = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'res.country.state', 'read', [extract_id(state_id)], {'fields': ['code']})
            if s_res: s_code = s_res[0].get('code')
    except: pass
    return c_code, s_code

def update_shopify_order_address(order_id, address_data, type='shipping_address'):
    """Updates Shopify Order with address data from Odoo"""
    url = f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders/{order_id}.json"
    headers = {
        "X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN'),
        "Content-Type": "application/json"
    }
    payload = {
        "order": {
            "id": order_id,
            type: address_data
        }
    }
    try:
        res = requests.put(url, json=payload, headers=headers)
        if res.status_code == 200:
            log_event('Shopify Update', 'Success', f"Updated {type} for Order {order_id} from Odoo data")
        else:
            log_event('Shopify Update', 'Error', f"Failed to update {type}: {res.text}")
    except Exception as e:
        log_event('Shopify Update', 'Error', f"Exception updating {type}: {str(e)}")

def process_order_data(data):
    """Core logic to sync a single order"""
    # 1. Extract Basic Info
    shopify_id = data.get('id')
    email = data.get('email') or data.get('contact_email')
    shopify_name = data.get('name')
    client_ref = f"ONLINE_{shopify_name}"
    company_id = get_config('odoo_company_id')
    
    # Try to fetch company_id from Odoo user if not set in config
    if not company_id and odoo:
        try:
            user_info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                'res.users', 'read', [[odoo.uid]], {'fields': ['company_id']})
            if user_info: company_id = user_info[0]['company_id'][0]
        except: pass

    # Check if order already exists
    try:
        existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
    except Exception as e: return False, f"Odoo Error: {str(e)}"

    # 2. Customer Resolution
    partner = None
    partner_id = None
    
    # A. Search by Email first
    if email:
        partner = odoo.search_partner_by_email(email)
    
    # B. If no email (POS?), Search by Name
    if not partner:
        cust_info = data.get('customer', {})
        s_first = cust_info.get('first_name', '')
        s_last = cust_info.get('last_name', '')
        s_full = f"{s_first} {s_last}".strip() or cust_info.get('default_address', {}).get('name')
        
        if s_full:
            try:
                # Search for exact name match in Odoo
                p_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                    'res.partner', 'search', [[['name', '=', s_full]]])
                if p_ids:
                    partner = {'id': p_ids[0], 'name': s_full}
            except: pass

    # 3. Handle Addresses (The "Two-Way Sync" Logic)
    
    # Extract Shopify Addresses
    s_ship_addr = data.get('shipping_address')
    s_bill_addr = data.get('billing_address')
    
    if partner:
        # EXISTING PARTNER FOUND: Use Odoo Data if Shopify is empty
        partner_id = extract_id(partner.get('id'))
        
        # Read full partner details to check for address/mobile
        p_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'res.partner', 'read', [partner_id], 
            {'fields': ['street', 'street2', 'city', 'zip', 'state_id', 'country_id', 'mobile', 'email', 'name', 'phone']})
        
        if p_data:
            p_rec = p_data[0]
            
            # --- Logic: If Shopify Missing Shipping, Use Odoo ---
            if not s_ship_addr and (p_rec.get('street') or p_rec.get('city')):
                # Fetch ISO codes for Shopify
                c_code, s_code = get_odoo_address_codes(p_rec.get('country_id'), p_rec.get('state_id'))
                
                # Construct Address Object for Shopify
                new_addr = {
                    "first_name": p_rec.get('name', '').split(' ')[0],
                    "last_name": " ".join(p_rec.get('name', '').split(' ')[1:]),
                    "address1": p_rec.get('street') or "",
                    "address2": p_rec.get('street2') or "",
                    "city": p_rec.get('city') or "",
                    "zip": p_rec.get('zip') or "",
                    "country_code": c_code or "",
                    "province_code": s_code or "",
                    "phone": p_rec.get('mobile') or p_rec.get('phone') or ""
                }
                
                # Update Shopify
                update_shopify_order_address(shopify_id, new_addr, 'shipping_address')
                
                # Use this for Odoo logic below
                s_ship_addr = new_addr 
                log_event('Order', 'Info', f"Backfilled Shipping Address from Odoo for {shopify_name}")

            # --- Logic: If Shopify Missing Billing, Use Odoo ---
            if not s_bill_addr and (p_rec.get('street') or p_rec.get('city')):
                 # (Re-use logic or fetch separate invoice address if complex, for now using main partner)
                 c_code, s_code = get_odoo_address_codes(p_rec.get('country_id'), p_rec.get('state_id'))
                 new_bill = {
                    "first_name": p_rec.get('name', '').split(' ')[0],
                    "last_name": " ".join(p_rec.get('name', '').split(' ')[1:]),
                    "address1": p_rec.get('street') or "",
                    "city": p_rec.get('city') or "",
                    "zip": p_rec.get('zip') or "",
                    "country_code": c_code or "",
                    "province_code": s_code or "",
                    "phone": p_rec.get('mobile') or ""
                 }
                 update_shopify_order_address(shopify_id, new_bill, 'billing_address')
                 s_bill_addr = new_bill

    else:
        # PARTNER NOT FOUND: Create New (COMPANY)
        cust_data = data.get('customer', {})
        def_address = s_bill_addr or s_ship_addr or cust_data.get('default_address') or {}

        first_name = cust_data.get('first_name') or def_address.get('first_name') or ''
        last_name = cust_data.get('last_name') or def_address.get('last_name') or ''
        full_name = f"{first_name} {last_name}".strip()
        
        if not full_name:
            full_name = def_address.get('name') or def_address.get('company') or email or f"Shopify Customer {shopify_name}"

        # ** NOTE: Mapping 'phone' from Shopify to 'mobile' in Odoo **
        vals = {
            'name': full_name, 
            'email': email or '', 
            'mobile': cust_data.get('phone') or def_address.get('phone'), # Map to Mobile
            'phone': '', # Keep Landline Empty
            'company_type': 'company', # Create as Company
            'street': def_address.get('address1'),
            'city': def_address.get('city'), 
            'zip': def_address.get('zip'), 
            'country_code': def_address.get('country_code')
        }
        if company_id: vals['company_id'] = int(company_id)

        try:
            partner_id = odoo.create_partner(vals)
            partner = {'id': partner_id, 'name': full_name, 'parent_id': False}
            log_event('Customer', 'Success', f"Created Company Contact: {full_name}")
        except Exception as e:
            log_event('Customer', 'Error', f"Create Failed: {e}")
            return False, f"Customer Error: {e}"
    
    # Ensure we have an ID
    if not partner_id and partner:
        partner_id = extract_id(partner.get('id'))

    # 4. Final Address Assignment for Odoo SO
    # Now that s_ship_addr and s_bill_addr might have been backfilled from Odoo, use them
    
    shipping_id = partner_id 
    if s_ship_addr:
        s_name = f"{s_ship_addr.get('first_name', '')} {s_ship_addr.get('last_name', '')}".strip() or s_ship_addr.get('name') or "Delivery Address"
        shipping_data = {
            'name': s_name, 
            'street': s_ship_addr.get('address1'), 
            'city': s_ship_addr.get('city'),
            'zip': s_ship_addr.get('zip'), 
            'mobile': s_ship_addr.get('phone'), # Map to Mobile
            'phone': '', # Keep Landline Empty
            'country_code': s_ship_addr.get('country_code'), 
            'email': email
        }
        try:
            found_id = odoo.find_or_create_child_address(partner_id, shipping_data, type='delivery')
            shipping_id = extract_id(found_id)
            if shipping_id != partner_id:
                 log_event('Customer', 'Info', f"Linked/Created Delivery Contact: {s_name}")
        except Exception as e:
            log_event('Customer', 'Warning', f"Delivery Addr Error: {e}")

    invoice_id = partner_id
    if s_bill_addr:
        b_name = f"{s_bill_addr.get('first_name', '')} {s_bill_addr.get('last_name', '')}".strip() or s_bill_addr.get('name') or "Invoice Address"
        billing_data = {
            'name': b_name, 
            'street': s_bill_addr.get('address1'), 
            'city': s_bill_addr.get('city'),
            'zip': s_bill_addr.get('zip'), 
            'mobile': s_bill_addr.get('phone'), # Map to Mobile
            'phone': '', # Keep Landline Empty
            'country_code': s_bill_addr.get('country_code'), 
            'email': email
        }
        try:
            found_id = odoo.find_or_create_child_address(partner_id, billing_data, type='invoice')
            invoice_id = extract_id(found_id)
            if invoice_id != partner_id:
                 log_event('Customer', 'Info', f"Linked/Created Invoice Contact: {b_name}")
        except Exception as e:
            log_event('Customer', 'Warning', f"Invoice Addr Error: {e}")
    elif s_ship_addr:
        invoice_id = shipping_id

    # 5. Build Lines
    lines = []
    for item in data.get('line_items', []):
        product_id = odoo.search_product_by_sku(item.get('sku'), company_id)
        if product_id:
            price = float(item.get('price', 0))
            qty = int(item.get('quantity', 1))
            disc = float(item.get('total_discount', 0))
            pct = (disc / (price * qty)) * 100 if price > 0 else 0.0
            lines.append((0, 0, {'product_id': product_id, 'product_uom_qty': qty, 'price_unit': price, 'name': item['name'], 'discount': pct}))
        else:
            log_event('Product', 'Warning', f"SKU {item.get('sku')} not found")

    for ship in data.get('shipping_lines', []):
        cost = float(ship.get('price', 0))
        title = ship.get('title', 'Shipping')
        ship_pid = odoo.search_product_by_name(title, company_id)
        
        if not ship_pid:
            ship_pid = odoo.search_product_by_name("Shipping", company_id)
            
        if not ship_pid:
            try:
                ship_pid = odoo.create_service_product(title, company_id)
                ship_pid = extract_id(ship_pid)
            except: pass
            
        if cost >= 0 and ship_pid:
            lines.append((0, 0, {'product_id': ship_pid, 'product_uom_qty': 1, 'price_unit': cost, 'name': title, 'is_delivery': True}))

    if not lines: return False, "No valid lines"
    
    # 6. Sync Order
    notes = [f"Note: {data.get('note', '')}"]
    gateways = data.get('payment_gateway_names') or ([data.get('gateway')] if data.get('gateway') else [])
    if gateways: notes.append(f"Payment: {', '.join(gateways)}")
    
    vals = {
        'name': client_ref, 'client_order_ref': client_ref,
        'partner_id': partner_id, 'partner_invoice_id': invoice_id, 'partner_shipping_id': shipping_id,
        'order_line': lines, 'user_id': odoo.uid, 'state': 'draft', 'note': "\n\n".join(notes)
    }
    if company_id: vals['company_id'] = int(company_id)
    
    try:
        if existing_ids:
            oid = extract_id(existing_ids[0])
            check = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'read', [oid], {'fields': ['state']})
            if check and check[0]['state'] in ['draft', 'sent']:
                vals['order_line'] = [(5, 0, 0)] + lines
                odoo.update_sale_order(oid, vals)
                log_event('Order', 'Success', f"Updated {client_ref}")
                return True, "Updated"
            else:
                log_event('Order', 'Skipped', f"Order {client_ref} is locked")
                return True, "Locked"
        else:
            odoo.create_sale_order(vals)
            log_event('Order', 'Success', f"Synced {client_ref}")
            return True, "Synced"
    except Exception as e:
        log_event('Order', 'Error', str(e))
        return False, str(e)

# --- AUTOMATION THREAD ---
def auto_sync_worker():
    """Background worker to auto-sync orders periodically"""
    with app.app_context():
        # Small initial delay to let server start
        time.sleep(10)
        print("--- Starting Auto-Sync Worker (Every 10 mins) ---")
        
        while True:
            try:
                # Fetch orders updated in the last 20 minutes to catch any misses
                updated_min = (datetime.utcnow() - timedelta(minutes=20)).isoformat()
                url = f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders.json?status=any&updated_at_min={updated_min}&limit=25"
                headers = {"X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN')}
                
                res = requests.get(url, headers=headers)
                if res.status_code == 200:
                    orders = res.json().get('orders', [])
                    for order in orders:
                        try:
                            # reuse the main process logic which checks if it exists before doing work
                            process_order_data(order)
                        except Exception as inner_e:
                            print(f"Auto-Sync Single Order Error: {inner_e}")
                else:
                    print(f"Auto-Sync Shopify Error: {res.status_code}")
                    
            except Exception as e:
                print(f"Auto-Sync Worker Error: {e}")
            
            # Sleep for 10 minutes
            time.sleep(600)

# --- ROUTES ---

@app.route('/')
def dashboard():
    try:
        logs_orders = SyncLog.query.filter(SyncLog.entity.in_(['Order', 'Order Cancel', 'Shopify Update'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_inventory = SyncLog.query.filter_by(entity='Inventory').order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_customers = SyncLog.query.filter_by(entity='Customer').order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_system = SyncLog.query.filter(SyncLog.entity.notin_(['Order', 'Order Cancel', 'Inventory', 'Customer', 'Shopify Update'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
    except:
        logs_orders = logs_inventory = logs_customers = logs_system = []
    
    env_locs = os.getenv('ODOO_STOCK_LOCATION_IDS', '0')
    default_locs = [int(x) for x in env_locs.split(',') if x.strip().isdigit()]
    
    current_settings = {
        "locations": get_config('inventory_locations', default_
