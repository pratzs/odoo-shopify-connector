import xmlrpc.client
import ssl

class OdooClient:
    def __init__(self, url, db, username, password):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        # Fix for some free hosting SSL contexts
        self.context = ssl._create_unverified_context()
        
        self.common = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/common', context=self.context)
        self.uid = self.common.authenticate(self.db, self.username, self.password, {})
        self.models = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/object', context=self.context)

    def search_partner_by_email(self, email):
        """Finds a customer and checks if they have a Parent Company"""
        ids = self.models.execute_kw(self.db, self.uid, self.password,
            'res.partner', 'search', [[['email', '=', email]]])
        if ids:
            partners = self.models.execute_kw(self.db, self.uid, self.password,
                'res.partner', 'read', [ids], {'fields': ['id', 'name', 'parent_id']})
            return partners[0]
        return None

    def search_product_by_sku(self, sku):
        """Finds product ID by Internal Reference"""
        ids = self.models.execute_kw(self.db, self.uid, self.password,
            'product.product', 'search', [[['default_code', '=', sku]]])
        return ids[0] if ids else None

    def get_changed_products(self, time_limit_str, location_id=None):
        """
        Finds products changed since the last sync.
        If location_id is provided, it reads stock specifically for that location.
        """
        # 1. Find products modified recently
        domain = [('write_date', '>', time_limit_str), ('type', '=', 'product')]
        product_ids = self.models.execute_kw(self.db, self.uid, self.password,
            'product.product', 'search', [domain])
        
        if not product_ids:
            return []

        # 2. Read their stock levels
        # We use the 'context' dictionary to tell Odoo "Read stock from THIS specific location"
        context = {}
        if location_id:
            context = {'location': location_id}

        return self.models.execute_kw(self.db, self.uid, self.password,
            'product.product', 'read', [product_ids],
            {'fields': ['default_code', 'qty_available'], 'context': context})

    def create_sale_order(self, order_vals):
        """Creates the order in Odoo"""
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'create', [order_vals])
