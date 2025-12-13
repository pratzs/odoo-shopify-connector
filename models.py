from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class Shop(db.Model):
    __tablename__ = 'shops'
    id = db.Column(db.Integer, primary_key=True)
    shop_url = db.Column(db.String(255), unique=True, nullable=False)
    access_token = db.Column(db.String(255), nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    installed_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Odoo Credentials (Multi-Tenant)
    odoo_url = db.Column(db.String(255))
    odoo_db = db.Column(db.String(100))
    odoo_username = db.Column(db.String(100))
    odoo_password = db.Column(db.String(100))
    odoo_company_id = db.Column(db.String(50)) # Fixed: Added this field

class AppSetting(db.Model):
    __tablename__ = 'app_settings'
    id = db.Column(db.Integer, primary_key=True)
    shop_id = db.Column(db.Integer, db.ForeignKey('shops.id', ondelete='CASCADE'), nullable=False)
    key = db.Column(db.String(50))
    value = db.Column(db.Text)

class SyncLog(db.Model):
    __tablename__ = 'sync_logs'
    id = db.Column(db.Integer, primary_key=True)
    shop_id = db.Column(db.Integer, db.ForeignKey('shops.id', ondelete='CASCADE')) # Fixed: Added this link
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    entity = db.Column(db.String(50))
    status = db.Column(db.String(20))
    message = db.Column(db.Text)

class ProductMap(db.Model):
    __tablename__ = 'product_map'
    shopify_variant_id = db.Column(db.String(50), primary_key=True)
    shop_id = db.Column(db.Integer, db.ForeignKey('shops.id', ondelete='CASCADE'))
    odoo_product_id = db.Column(db.Integer, nullable=False)
    sku = db.Column(db.String(50))
    last_synced_at = db.Column(db.DateTime, default=datetime.utcnow)

class CustomerMap(db.Model):
    __tablename__ = 'customer_map'
    shopify_customer_id = db.Column(db.String(50), primary_key=True)
    shop_id = db.Column(db.Integer, db.ForeignKey('shops.id', ondelete='CASCADE'))
    odoo_partner_id = db.Column(db.Integer, nullable=False)
    email = db.Column(db.String(100))
