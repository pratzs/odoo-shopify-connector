from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

# --- MULTI-TENANT SHOP MODEL ---
class Shop(db.Model):
    __tablename__ = 'shops'
    id = db.Column(db.Integer, primary_key=True)
    shop_url = db.Column(db.String(255), unique=True, nullable=False) # e.g. "my-store.myshopify.com"
    access_token = db.Column(db.String(255), nullable=False)
    installed_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    
    # Store Odoo credentials PER SHOP (since each user has their own Odoo)
    odoo_url = db.Column(db.String(255))
    odoo_db = db.Column(db.String(255))
    odoo_username = db.Column(db.String(255))
    odoo_password = db.Column(db.String(255))

# --- LINKED MODELS ---
class ProductMap(db.Model):
    __tablename__ = 'product_map'
    id = db.Column(db.Integer, primary_key=True)
    # Link to Shop
    shop_id = db.Column(db.Integer, db.ForeignKey('shops.id'), nullable=False)
    
    shopify_variant_id = db.Column(db.String(50))
    odoo_product_id = db.Column(db.Integer, nullable=False)
    sku = db.Column(db.String(50), index=True)
    last_synced_at = db.Column(db.DateTime, default=datetime.utcnow)

class CustomerMap(db.Model):
    __tablename__ = 'customer_map'
    id = db.Column(db.Integer, primary_key=True)
    shop_id = db.Column(db.Integer, db.ForeignKey('shops.id'), nullable=False)
    
    shopify_customer_id = db.Column(db.String(50))
    odoo_partner_id = db.Column(db.Integer, nullable=False)
    email = db.Column(db.String(100), index=True)

class SyncLog(db.Model):
    __tablename__ = 'sync_logs'
    id = db.Column(db.Integer, primary_key=True)
    shop_id = db.Column(db.Integer, db.ForeignKey('shops.id'), nullable=True) # Nullable for system logs
    
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    entity = db.Column(db.String(50)) 
    status = db.Column(db.String(20)) 
    message = db.Column(db.Text)

class AppSetting(db.Model):
    __tablename__ = 'app_settings'
    id = db.Column(db.Integer, primary_key=True)
    shop_id = db.Column(db.Integer, db.ForeignKey('shops.id'), nullable=False)
    
    key = db.Column(db.String(50))
    value = db.Column(db.Text)
