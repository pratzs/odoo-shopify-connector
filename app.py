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

app = Flask(__name__)

# --- CONFIGURATION ---
# FIX: Handle Supabase connection string for pg8000 driver
database_url = os.getenv('DATABASE_URL', 'sqlite:///local.db')

if database_url:
    # If it starts with postgres://, change to postgresql+pg8000://
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+pg8000://", 1)
    # If it starts with postgresql://, change to postgresql+pg8000://
    elif database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+pg8000://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

SHOPIFY_LOCATION_ID = int(os.getenv('SHOPIFY_WAREHOUSE_ID', '0'))

db.init_app(app)

# Initialize Odoo
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
    """Retrieve setting from DB, fallback to default"""
    try:
        setting = AppSetting.query.get(key)
        # Handle cases where value might be a simple string or JSON
        try:
            return json.loads(setting.value)
        except:
            return setting.value
    except:
        return default

def set_config(key, value):
    """Save setting to DB"""
    try:
        setting = AppSetting.query.get(key)
        if not setting:
            setting = AppSetting(key=key)
            db.session.add(setting)
        # Store complex data as JSON string
        setting.value = json.dumps(value)
        db.session.commit()
        return True
    except Exception as e:
        print(f"Error saving config: {e}")
        return False

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
    except Exception as e:
        print(f"DB LOG ERROR: {e}")

def process_order_data(data):
    """Core logic to sync a single order"""
    email = data.get('email') or data.get('contact_email')
    shopify_name = data.get('name')
    client_ref = f"ONLINE_{shopify_name}"
    
    # Load Company ID from Settings to prevent cross-company errors
    company_id = get_config('odoo_company_id')
    
    # FALLBACK: If no company configured, try to auto-detect from API User
    if not company_id and odoo:
        try:
            user_info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                'res.users', 'read', [[odoo.uid]], {'fields': ['company_id']})
            if user_info:
                company_id = user_info[0]['company_id'][0] # [ID, Name]
        except Exception as e:
            print(f"DEBUG: Failed to auto-detect company: {e}")

    # 1. Check if Order Exists
    existing_ids = []
    try:
        existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
    except Exception as e:
        return False, f"Odoo Connection Error: {str(e)}"

    # 2. Customer Sync & Creation
    partner = odoo.search_partner_by_email(email)
    
    # Create Customer if missing
    if not partner:
        cust_data = data.get('customer', {})
        def_address = data.get('billing_address') or data.get('shipping_address') or {}
        
        name = f"{cust_data.get('first_name', '')} {cust_data.get('last_name', '')}".strip()
        if not name: name = def_address.get('name') or email
            
        new_partner_vals = {
            'name': name,
            'email': email,
            'phone': cust_data.get('phone') or def_address.get('phone'),
            'company_type': 'company', # Create as Parent Company
            'street': def_address.get('address1'),
            'city': def_address.get('city'),
            'zip': def_address.get('zip'),
            'country_code': def_address.get('country_code'),
        }
        
        if company_id:
            new_partner_vals['company_id'] = int(company_id)

        try:
            partner_id = odoo.create_partner(new_partner_vals)
            # Minimal partner object
            partner = {'id': partner_id, 'name': name, 'parent_id': False}
            log_event('Customer', 'Success', f"Created New Customer: {name}")
        except Exception as e:
            log_event('Customer', 'Error', f"Failed to create customer {email}: {e}")
            return False, f"Customer Creation Failed: {e}"
    else:
        if partner.get('parent_id'):
            partner_id = partner['parent_id'][0]
        else:
            partner_id = partner['id']

    # 2B. Handle Delivery Address (Child Contact)
    ship_addr = data.get('shipping_address', {})
    shipping_id = partner_id # Default to parent
    
    if ship_addr:
        # Improved Name Logic
        s_name = f"{ship_addr.get('first_name', '')} {ship_addr.get('last_name', '')}".strip()
        if not s_name: s_name = ship_addr.get('name', 'Delivery Address')

        shipping_data = {
            'name': s_name,
            'street': ship_addr.get('address1'),
            'city': ship_addr.get('city'),
            'zip': ship_addr.get('zip'),
            'phone': ship_addr.get('phone'),
            'country_code': ship_addr.get('country_code'),
            'email': email
        }
        try:
            found_shipping_id = odoo.find_or_create_child_address(partner_id, shipping_data, type='delivery')
            if found_shipping_id:
                shipping_id = found_shipping_id
                print(f"DEBUG: Delivery Address ID: {shipping_id}")
        except Exception as e:
            log_event('Customer', 'Warning', f"Could not create Delivery Address: {e}")
            # shipping_id remains partner_id

    # 2C. Handle Invoice Address (Child Contact)
    bill_addr = data.get('billing_address') or ship_addr # Fallback to shipping if missing
    invoice_id = partner_id # Default to parent

    if bill_addr:
        b_name = f"{bill_addr.get('first_name', '')} {bill_addr.get('last_name', '')}".strip()
        if not b_name: b_name = bill_addr.get('name', 'Invoice Address')
        
        billing_data = {
            'name': b_name,
            'street': bill_addr.get('address1'),
            'city': bill_addr.get('city'),
            'zip': bill_addr.get('zip'),
            'phone': bill_addr.get('phone'),
            'country_code': bill_addr.get('country_code'),
            'email': email
        }
        try:
            found_invoice_id = odoo.find_or_create_child_address(partner_id, billing_data, type='invoice')
            if found_invoice_id:
                invoice_id = found_invoice_id
                print(f"DEBUG: Invoice Address ID: {invoice_id}")
        except Exception as e:
            log_event('Customer', 'Warning', f"Could not create Invoice Address: {e}")
            # invoice_id remains partner_id

    # 3. Build Lines
    lines = []
    # Products
    for item in data.get('line_items', []):
        sku = item.get('sku')
        if not sku: continue
        
        # Pass company_id to enforce selection from correct company
        product_id = odoo.search_product_by_sku(sku, company_id)
        
        if product_id:
            price = float(item.get('price', 0))
            qty = int(item.get('quantity', 1))
            discount = float(item.get('total_discount', 0))
            pct = (discount / (price * qty)) * 100 if price > 0 else 0.0
            
            lines.append((0, 0, {
                'product_id': product_id, 'product_uom_qty': qty,
                'price_unit': price, 'name': item['name'], 'discount': pct
            }))
        else:
            log_event('Product', 'Warning', f"SKU {sku} not found in Company {company_id or 'All'}")

    # Shipping (Auto-Create if Missing)
    for ship in data.get('shipping_lines', []):
        cost = float(ship.get('price', 0))
        title = ship.get('title', 'Shipping')
        
        # 1. Try to find existing
        ship_pid = odoo.search_product_by_name(title, company_id)
        
        # 2. If NOT found, try generic
        if not ship_pid:
            ship_pid = odoo.search_product_by_name("Shipping", company_id)
        
        # 3. If STILL not found, CREATE IT
        if not ship_pid:
            try:
                print(f"DEBUG: Creating new shipping product '{title}'")
                ship_pid = odoo.create_service_product(title, company_id)
                if isinstance(ship_pid, list): ship_pid = ship_pid[0]
            except Exception as e:
                print(f"ERROR creating shipping product: {e}")
                log_event('Product', 'Error', f"Could not create shipping product '{title}': {str(e)}")

        if cost >= 0 and ship_pid:
            lines.append((0, 0, {
                'product_id': ship_pid, 
                'product_uom_qty': 1, 
                'price_unit': cost, 
                'name': title, 
                'is_delivery': True
            }))

    if not lines: return False, "No valid lines"

    # 4. Create Order with Notes
    notes = []
    if data.get('note'): notes.append(f"Note: {data['note']}")
    
    gateways = data.get('payment_gateway_names')
    if not gateways and data.get('gateway'):
        gateways = [data.get('gateway')]
    
    if gateways: notes.append(f"Payment: {', '.join(gateways)}")
    
    try:
        vals = {
            'name': client_ref, 'client_order_ref': client_ref,
            'partner_id': partner_id,           
            'partner_invoice_id': invoice_id,   
            'partner_shipping_id': shipping_id, 
            'order_line': lines, 
            'user_id': odoo.uid, 
            'state': 'draft', 
            'note': "\n\n".join(notes)
        }
        
        if company_id:
             vals['company_id'] = int(company_id)

        # UPDATE or CREATE Logic
        if existing_ids:
            # Check state before updating
            order_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                'sale.order', 'read', [existing_ids[0]], {'fields': ['state']})
            
            if order_data and order_data[0]['state'] in ['draft', 'sent']:
                # Update existing draft order
                # Use (5, 0, 0) to remove old lines, then add new ones
                vals['order_line'] = [(5, 0, 0)] + lines
                odoo.update_sale_order(existing_ids[0], vals)
                log_event('Order', 'Success', f"Updated existing Order {client_ref}")
                return True, "Updated"
            else:
                log_event('Order', 'Skipped', f"Order {client_ref} exists and is locked ({order_data[0]['state']}).")
                return True, "Order Locked"
        else:
            # Create new
            odoo.create_sale_order(vals)
            log_event('Order', 'Success', f"Synced {client_ref}")
            return True, "Synced"

    except Exception as e:
        log_event('Order', 'Error', f"Failed {client_ref}: {str(e)}")
        return False, str(e)

# --- ROUTES ---

@app.route('/')
def dashboard():
    try:
        logs_orders = SyncLog.query.filter(SyncLog.entity.in_(['Order', 'Order Cancel'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_inventory = SyncLog.query.filter_by(entity='Inventory').order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_customers = SyncLog.query.filter_by(entity='Customer').order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_system = SyncLog.query.filter(SyncLog.entity.notin_(['Order', 'Order Cancel', 'Inventory', 'Customer'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
    except:
        logs_orders = logs_inventory = logs_customers = logs_system = []
    
    # Get Settings
    env_locs = os.getenv('ODOO_STOCK_LOCATION_IDS', '0')
    default_locs = [int(x) for x in env_locs.split(',') if x.strip().isdigit()]
    
    current_settings = {
        "locations": get_config('inventory_locations', default_locs),
        "field": get_config('inventory_field', 'qty_available'),
        "sync_zero": get_config('sync_zero_stock', False),
        "combine_committed": get_config('combine_committed', False),
        "company_id": get_config('odoo_company_id', None),
        "cust_direction": get_config('cust_direction', 'bidirectional'),
        "cust_auto_sync": get_config('cust_auto_sync', True)
    }

    odoo_status = True if odoo else False
    return render_template('dashboard.html', 
                           logs_orders=logs_orders, logs_inventory=logs_inventory, 
                           logs_customers=logs_customers, logs_system=logs_system,
                           odoo_status=odoo_status, current_settings=current_settings)

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
    return jsonify({"message": "Settings Saved"})

@app.route('/sync/inventory', methods=['GET'])
def sync_inventory():
    if not odoo: return jsonify({"error": "Offline"}), 500
    
    # Load Settings from DB
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
    process_order_data(request.json)
    return "Received", 200

@app.route('/webhook/orders/cancelled', methods=['POST'])
def order_cancelled_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
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

if __name__ == '__main__':
    app.run(debug=True)
