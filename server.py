import requests
import json

class ShopifyClient:
    def __init__(self, shop_url, access_token):
        self.base_url = f"https://{shop_url}/admin/api/2024-01"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json"
        }

    def get_product_variant_id_by_sku(self, sku):
        """Finds a Shopify Variant ID using SKU (Critical for syncing stock)"""
        query = """
        {
          productVariants(first: 1, query: "sku:%s") {
            edges {
              node {
                id
                inventoryItem {
                  id
                }
              }
            }
          }
        }
        """ % sku
        response = requests.post(
            f"https://{self.shop_url}/admin/api/2024-01/graphql.json", # Fix URL in actual implementation
            headers=self.headers,
            json={'query': query}
        )
        # ... (Error handling and parsing logic would go here, simplified for brevity)
        # Returns inventory_item_id
        return None 

    def update_inventory(self, location_id, inventory_item_id, new_quantity):
        """Updates stock levels in Shopify (Replicates Inventory Sync)"""
        payload = {
            "location_id": location_id,
            "inventory_item_id": inventory_item_id,
            "available": new_quantity
        }
        response = requests.post(
            f"{self.base_url}/inventory_levels/set.json",
            headers=self.headers,
            json=payload
        )
        return response.status_code == 200

    def update_product_price(self, shopify_product_id, new_price):
        """Updates Price (Replicates Product Sync Settings)"""
        payload = {
            "product": {
                "id": shopify_product_id,
                "variants": [{"price": new_price}] 
                # Note: This is simplified. In reality, you'd loop through variants.
            }
        }
        requests.put(
            f"{self.base_url}/products/{shopify_product_id}.json",
            headers=self.headers,
            json=payload
        )

    def fulfill_order(self, shopify_order_id, tracking_number, carrier):
        """Marks order as fulfilled in Shopify when Odoo ships it"""
        payload = {
            "fulfillment": {
                "location_id": 123456789, # Your VJ Trading Warehouse ID
                "tracking_number": tracking_number,
                "tracking_company": carrier,
                "line_items_by_fulfillment_order": [] # Logic to fetch fulfillment order ID needed
            }
        }
        # Fulfillment logic is complex in REST Admin API 2023+, requires FulfillmentOrder ID
        pass
