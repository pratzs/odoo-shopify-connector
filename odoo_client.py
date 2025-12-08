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

    def search_product_by_sku(self, sku, company_id=None):
        """
        Finds product ID by Internal Reference (SKU).
        If company_id is provided, checks for products in that company OR shared products (False).
        """
        domain = [['default_code', '=', sku]]
        if company_id:
            domain.insert(0, '|') # OR operator
            domain.append(['company_id', '=', int(company_id)])
            domain.append(['company_id', '=', False]) # Shared products

        ids = self.models.execute_kw(self.db, self.uid, self.password,
            'product.product', 'search', [domain])
        return ids[0] if ids else None

    def search_product_by_name(self, name, company_id=None):
        """
        Finds product ID by Name (Useful for Shipping Methods).
        If company_id is provided, filters by company.
        """
        domain = [['name', 'ilike', name]]
        if company_id:
            domain.insert(0, '|') # OR operator
            domain.append(['company_id', '=', int(company_id)])
            domain.append(['company_id', '=', False])

        ids = self.models.execute_kw(self.db, self.uid, self.password,
            'product.product', 'search', [domain])
        return ids[0] if ids else None

    def get_changed_products(self, time_limit_str, company_id=None):
        """
        Finds IDs of products changed recently.
        Used by the sync job to know which products to check.
        """
        domain = [('write_date', '>', time_limit_str), ('type', '=', 'product')]
        if company_id:
            domain.insert(0, '|')
            domain.append(['company_id', '=', int(company_id)])
            domain.append(['company_id', '=', False])

        return self.models.execute_kw(self.db, self.uid, self.password,
            'product.product', 'search', [domain])

    def get_companies(self):
        """Fetches list of all companies available in Odoo"""
        return self.models.execute_kw(self.db, self.uid, self.password,
            'res.company', 'search_read', [[]], 
            {'fields': ['id', 'name']})

    def get_locations(self, company_id=None):
        """
        Fetches internal locations.
        If company_id is provided, filters locations to that specific company.
        """
        if not company_id:
            return [] # Return empty if no company selected to save memory

        # Filter for Internal Usage AND specific Company
        domain = [['usage', '=', 'internal'], ['company_id', '=', int(company_id)]]
        
        return self.models.execute_kw(self.db, self.uid, self.password,
            'stock.location', 'search_read', [domain], 
            {'fields': ['id', 'complete_name', 'company_id']})

    def get_total_qty_for_locations(self, product_id, location_ids, field_name='qty_available'):
        """
        Calculates total stock for a product across multiple Odoo locations.
        field_name can be 'qty_available' (On Hand) or 'virtual_available' (Forecasted).
        """
        total_qty = 0
        for loc_id in location_ids:
            # We use the 'context' dictionary to tell Odoo "Read stock from THIS specific location"
            context = {'location': loc_id}
            data = self.models.execute_kw(self.db, self.uid, self.password,
                'product.product', 'read', [product_id],
                {'fields': [field_name], 'context': context})
            if data:
                total_qty += data[0].get(field_name, 0)
        return total_qty

    def create_sale_order(self, order_vals):
        """Creates the order in Odoo"""
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'create', [order_vals])

    def update_sale_order(self, order_id, order_vals):
        """Updates an existing order"""
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'write', [[order_id], order_vals])

    def post_message(self, order_id, message):
        """Adds a note to the order chatter"""
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'message_post', [order_id], {'body': message})
