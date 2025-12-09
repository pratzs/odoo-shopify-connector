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
        # Strictly Active
        ids = self.models.execute_kw(self.db, self.uid, self.password,
            'res.partner', 'search', [[['email', '=', email], ['active', '=', True]]])
        if ids:
            partners = self.models.execute_kw(self.db, self.uid, self.password,
                'res.partner', 'read', [ids], {'fields': ['id', 'name', 'parent_id', 'user_id']})
            return partners[0]
        return None

    def get_partner_salesperson(self, partner_id):
        data = self.models.execute_kw(self.db, self.uid, self.password,
            'res.partner', 'read', [[partner_id]], {'fields': ['user_id']})
        if data and data[0].get('user_id'):
            return data[0]['user_id'][0] 
        return None

    def create_partner(self, vals):
        self._resolve_country(vals)
        return self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'create', [vals])

    def find_or_create_child_address(self, parent_id, address_data, type='delivery'):
        domain = [
            ['parent_id', '=', parent_id],
            ['type', '=', type],
            ['street', '=', address_data.get('street')],
            ['active', '=', True]
        ]
        existing_ids = self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'search', [domain])

        if existing_ids:
            return existing_ids[0]
        
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
        code = vals.get('country_code')
        if code:
            ids = self.models.execute_kw(self.db, self.uid, self.password, 'res.country', 'search', [[['code', '=', code]]])
            if not ids:
                 ids = self.models.execute_kw(self.db, self.uid, self.password, 'res.country', 'search', [[['name', 'ilike', code]]])
            if ids:
                vals['country_id'] = ids[0]
            del vals['country_code']

    def search_product_by_sku(self, sku, company_id=None):
        """Strictly searches for ACTIVE products only."""
        domain = [['default_code', '=', sku], ['active', '=', True]]
        if company_id:
            domain.append('|')
            domain.append(['company_id', '=', int(company_id)])
            domain.append(['company_id', '=', False])
            
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])
        return ids[0] if ids else None

    def check_product_exists_by_sku(self, sku, company_id=None):
        """Checks if a product exists (Active OR Archived) to prevent creation errors."""
        # Note: We use '|', ('active', '=', True), ('active', '=', False) to find both
        domain = [['default_code', '=', sku], '|', ['active', '=', True], ['active', '=', False]]
        if company_id:
            domain.append('|')
            domain.append(['company_id', '=', int(company_id)])
            domain.append(['company_id', '=', False])
            
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])
        return ids[0] if ids else None

    def search_product_by_name(self, name, company_id=None):
        domain = [['name', 'ilike', name], ['active', '=', True]]
        if company_id:
            domain.append('|')
            domain.append(['company_id', '=', int(company_id)])
            domain.append(['company_id', '=', False])
            
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])
        return ids[0] if ids else None

    def create_service_product(self, name, company_id=None):
        vals = {
            'name': name, 'type': 'service', 'invoice_policy': 'order', 
            'list_price': 0.0, 'sale_ok': True, 'purchase_ok': False
        }
        if company_id: vals['company_id'] = int(company_id)
        return self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'create', [vals])

    def create_product(self, vals):
        if 'type' not in vals:
            vals['type'] = 'product'
        if 'invoice_policy' not in vals:
            vals['invoice_policy'] = 'delivery'
        return self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'create', [vals])

    def get_all_products(self, company_id=None):
        """Fetches ALL products (Active & Archived) for Master Sync"""
        # Note: We fetch both active=True and active=False to sync archival status
        domain = [('type', '=', 'product'), ('default_code', '!=', False), '|', ('active', '=', True), ('active', '=', False)]
        if company_id:
            # Reconstruct for Company logic
            # (Type=Product AND SKU!=False AND (Active=True OR Active=False) AND (Company=ID OR Company=False))
            domain = [
                '&', '&', '&',
                ('type', '=', 'product'),
                ('default_code', '!=', False),
                '|', ('active', '=', True), ('active', '=', False),
                '|', ('company_id', '=', int(company_id)), ('company_id', '=', False)
            ]
        
        # We include 'active' field to know status
        fields = ['id', 'name', 'default_code', 'list_price', 'standard_price', 'weight', 'description_sale', 'active']
        return self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search_read', [domain], {'fields': fields})

    def get_changed_products(self, time_limit_str, company_id=None):
        """Fetches changed products (Active & Archived) since time limit"""
        domain = [('write_date', '>', time_limit_str), ('type', '=', 'product'), '|', ('active', '=', True), ('active', '=', False)]
        if company_id:
            domain = [
                '&', '&', '&',
                ('write_date', '>', time_limit_str), 
                ('type', '=', 'product'),
                '|', ('active', '=', True), ('active', '=', False),
                '|', 
                ('company_id', '=', int(company_id)), 
                ('company_id', '=', False)
            ]
            
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
