import xmlrpc.client
import ssl

class OdooClient:
    def __init__(self, url, db, username, password):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        self.context = ssl._create_unverified_context()
        
        self.common = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/common', context=self.context)
        self.uid = self.common.authenticate(self.db, self.username, self.password, {})
        self.models = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/object', context=self.context)

    def search_partner_by_email(self, email):
        ids = self.models.execute_kw(self.db, self.uid, self.password,
            'res.partner', 'search', [[['email', '=', email]]])
        if ids:
            partners = self.models.execute_kw(self.db, self.uid, self.password,
                'res.partner', 'read', [ids], {'fields': ['id', 'name', 'parent_id']})
            return partners[0]
        return None

    def search_product_by_sku(self, sku):
        ids = self.models.execute_kw(self.db, self.uid, self.password,
            'product.product', 'search', [[['default_code', '=', sku]]])
        return ids[0] if ids else None

    def search_product_by_name(self, name):
        ids = self.models.execute_kw(self.db, self.uid, self.password,
            'product.product', 'search', [[['name', 'ilike', name]]])
        return ids[0] if ids else None

    def get_changed_products(self, time_limit_str):
        domain = [('write_date', '>', time_limit_str), ('type', '=', 'product')]
        return self.models.execute_kw(self.db, self.uid, self.password,
            'product.product', 'search', [domain])

    def get_companies(self):
        """Fetches list of all companies available in Odoo"""
        return self.models.execute_kw(self.db, self.uid, self.password,
            'res.company', 'search_read', [[]], 
            {'fields': ['id', 'name']})

    def get_locations(self, company_id=None):
        """Fetches internal locations, optionally filtered by Company ID"""
        domain = [['usage', '=', 'internal']]
        
        if company_id:
            # Filter by the specific company selected in Dashboard
            domain.append(['company_id', '=', int(company_id)])
        
        return self.models.execute_kw(self.db, self.uid, self.password,
            'stock.location', 'search_read', [domain], 
            {'fields': ['id', 'complete_name', 'company_id']})

    def get_total_qty_for_locations(self, product_id, location_ids, field_name='qty_available'):
        total_qty = 0
        for loc_id in location_ids:
            context = {'location': loc_id}
            data = self.models.execute_kw(self.db, self.uid, self.password,
                'product.product', 'read', [product_id],
                {'fields': [field_name], 'context': context})
            if data:
                total_qty += data[0].get(field_name, 0)
        return total_qty

    def create_sale_order(self, order_vals):
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'create', [order_vals])

    def update_sale_order(self, order_id, order_vals):
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'write', [[order_id], order_vals])

    def post_message(self, order_id, message):
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'message_post', [order_id], {'body': message})
