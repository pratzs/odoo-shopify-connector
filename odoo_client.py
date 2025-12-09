import xmlrpc.client
import ssl

class OdooClient:
    def __init__(self, url, db, username, password):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        self.context = ssl._create_unverified_context()
        
        # Enable allow_none to handle empty Shopify fields without crashing
        self.common = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/common', context=self.context, allow_none=True)
        self.uid = self.common.authenticate(self.db, self.username, self.password, {})
        self.models = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/object', context=self.context, allow_none=True)

    def search_partner_by_email(self, email):
        ids = self.models.execute_kw(self.db, self.uid, self.password,
            'res.partner', 'search', [[['email', '=', email]]])
        if ids:
            partners = self.models.execute_kw(self.db, self.uid, self.password,
                'res.partner', 'read', [ids], {'fields': ['id', 'name', 'parent_id', 'user_id']})
            return partners[0]
        return None

    def get_partner_salesperson(self, partner_id):
        """Fetches the Salesperson (user_id) for a specific partner/company"""
        data = self.models.execute_kw(self.db, self.uid, self.password,
            'res.partner', 'read', [[partner_id]], {'fields': ['user_id']})
        if data and data[0].get('user_id'):
            # user_id is returned as a tuple (id, name), we want the ID at index 0
            return data[0]['user_id'][0] 
        return None

    def create_partner(self, vals):
        """Creates a new contact/company in Odoo"""
        # Clean vals: Ensure no None types for critical fields if needed, 
        # though allow_none handles most. We resolve country first.
        self._resolve_country(vals)
        return self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'create', [vals])

    def find_or_create_child_address(self, parent_id, address_data, type='delivery'):
        """Checks if a child address exists. If not, creates it."""
        # 1. Search existing children to avoid duplicates
        # We only search by Street to be lenient (Zip can be inconsistent)
        domain = [
            ['parent_id', '=', parent_id],
            ['type', '=', type],
            ['street', '=', address_data.get('street')]
        ]
        existing_ids = self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'search', [domain])

        if existing_ids:
            return existing_ids[0]
        
        # 2. Create New Child
        vals = {
            'parent_id': parent_id,
            'type': type,
            'name': address_data.get('name') or "Delivery Address",
            'street': address_data.get('street'),
            'city': address_data.get('city'),
            'zip': address_data.get('zip'),
            'country_code': address_data.get('country_code'),
            'phone': address_data.get('phone'),
            'email': address_data.get('email')
        }
        
        self._resolve_country(vals)

        return self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'create', [vals])

    def _resolve_country(self, vals):
        """Helper to find Odoo Country ID from ISO code"""
        code = vals.get('country_code')
        if code:
            # Try searching by Code (NZ)
            ids = self.models.execute_kw(self.db, self.uid, self.password, 'res.country', 'search', [[['code', '=', code]]])
            if not ids:
                 # Try searching by Name (New Zealand)
                 ids = self.models.execute_kw(self.db, self.uid, self.password, 'res.country', 'search', [[['name', 'ilike', code]]])
            
            if ids:
                vals['country_id'] = ids[0]
            
            # Remove the code key as Odoo doesn't use it for creation
            del vals['country_code']

    def search_product_by_sku(self, sku, company_id=None):
        if company_id:
            domain = ['&', ['default_code', '=', sku], '|', ['company_id', '=', int(company_id)], ['company_id', '=', False]]
        else:
            domain = [['default_code', '=', sku]]
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])
        return ids[0] if ids else None

    def search_product_by_name(self, name, company_id=None):
        if company_id:
            domain = ['&', ['name', 'ilike', name], '|', ['company_id', '=', int(company_id)], ['company_id', '=', False]]
        else:
            domain = [['name', 'ilike', name]]
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])
        return ids[0] if ids else None

    def create_service_product(self, name, company_id=None):
        vals = {
            'name': name, 'type': 'service', 'invoice_policy': 'order', 
            'list_price': 0.0, 'sale_ok': True, 'purchase_ok': False
        }
        if company_id: vals['company_id'] = int(company_id)
        return self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'create', [vals])

    def get_changed_products(self, time_limit_str, company_id=None):
        if company_id:
            domain = ['&', '&', ['write_date', '>', time_limit_str], ['type', '=', 'product'], '|', ['company_id', '=', int(company_id)], ['company_id', '=', False]]
        else:
            domain = [('write_date', '>', time_limit_str), ('type', '=', 'product')]
        return self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])

    def get_companies(self):
        return self.models.execute_kw(self.db, self.uid, self.password, 'res.company', 'search_read', [[]], {'fields': ['id', 'name']})

    def get_locations(self, company_id=None):
        if not company_id: return []
        domain = [['usage', '=', 'internal'], ['company_id', '=', int(company_id)]]
        return self.models.execute_kw(self.db, self.uid, self.password, 'stock.location', 'search_read', [domain], {'fields': ['id', 'complete_name', 'company_id']})

    def get_total_qty_for_locations(self, product_id, location_ids, field_name='qty_available'):
        total_qty = 0
        for loc_id in location_ids:
            context = {'location': loc_id}
            data = self.models.execute_kw(self.db, self.uid, self.password,
                'product.product', 'read', [product_id],
                {'fields': [field_name], 'context': context})
            if data: total_qty += data[0].get(field_name, 0)
        return total_qty

    def create_sale_order(self, order_vals):
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'create', [order_vals])

    def update_sale_order(self, order_id, order_vals):
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'write', [[order_id], order_vals])

    def post_message(self, order_id, message):
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'message_post', [order_id], {'body': message})
