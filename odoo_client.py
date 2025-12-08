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
        """Finds a customer by email. Returns the first match."""
        # We search for the email strictly
        ids = self.models.execute_kw(self.db, self.uid, self.password,
            'res.partner', 'search', [[['email', '=', email]]])
        
        if ids:
            # Get details to check if it has a parent
            partners = self.models.execute_kw(self.db, self.uid, self.password,
                'res.partner', 'read', [ids], {'fields': ['id', 'name', 'parent_id']})
            return partners[0]
        return None

    def create_partner(self, vals):
        """Creates a new contact/company in Odoo"""
        return self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'create', [vals])

    def find_or_create_child_address(self, parent_id, address_data, type='delivery'):
        """
        Checks if a child address exists for this parent. If not, creates it.
        address_data expected: {'street': ..., 'city': ..., 'zip': ..., 'phone': ..., 'name': ...}
        """
        # 1. Search existing children of this parent
        domain = [
            ['parent_id', '=', parent_id],
            ['type', '=', type],
            ['street', '=', address_data.get('street')],
            ['zip', '=', address_data.get('zip')]
        ]
        
        existing_ids = self.models.execute_kw(self.db, self.uid, self.password,
            'res.partner', 'search', [domain])

        if existing_ids:
            return existing_ids[0]
        
        # 2. If not found, Create New Child
        vals = {
            'parent_id': parent_id,
            'type': type,
            'name': address_data.get('name'),
            'street': address_data.get('street'),
            'city': address_data.get('city'),
            'zip': address_data.get('zip'),
            'country_code': address_data.get('country_code'), # e.g. 'NZ'
            'phone': address_data.get('phone'),
            'email': address_data.get('email') # Child often shares email or has none
        }
        
        # Resolve Country ID if code provided
        if vals.get('country_code'):
            country_ids = self.models.execute_kw(self.db, self.uid, self.password,
                'res.country', 'search', [[['code', '=', vals['country_code']]]])
            if country_ids:
                vals['country_id'] = country_ids[0]
            del vals['country_code']

        return self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'create', [vals])

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
        vals = {'name': name, 'type': 'service', 'invoice_policy': 'order', 'list_price': 0.0, 'sale_ok': True}
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
