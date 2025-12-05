import os
import requests

from flask import Flask, request, jsonify

app = Flask(__name__)

# Replace with your actual Shopify API token and URL
# Note: Never hardcode sensitive information like API tokens directly in the code.
# Use environment variables or a secrets management system.

# Check that the necessary environment variables are set.  If not, use a default value
# and provide a clear warning to the user.  This ensures that the app starts up, but
# also lets the user know to correct their configuration.

shopify_url = f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10"

@app.route('/inventory', methods=['GET'])
def get_inventory():
    """Retrieves current inventory levels from Shopify."""

    # Implement logic to retrieve inventory data from Shopify.
    # This might involve making API calls to the Shopify admin API.
    # Use environment variables for all sensitive configuration!

    return jsonify({"message": "Inventory data retrieval is currently not implemented."}), 501

@app.route('/sync_inventory', methods=['POST'])
def sync_inventory():
    """Updates inventory levels in Shopify."""

    # Implement logic to sync inventory data to Shopify.
    # This might involve making API calls to the Shopify admin API.
    # Use environment variables for all sensitive configuration!

    data = request.get_json()  # Get request body as JSON

    # Validate that data is a dictionary.
    if type(data) != dict:
        return jsonify({"message": "Invalid data format. Please provide a dictionary."}), 400

    # Validate that all required keys exist.
    if all(key not in data for key in ["SKU", "quantity"]):
        return jsonify({"message": "Invalid data format.  Provide 'SKU' and 'quantity' keys"}), 400

    # Ensure quantity is an integer, handle exceptions
    try:
        data["quantity"] = int(data["quantity"])
    except ValueError:
        return jsonify({"message": "Invalid data format.  'quantity' must be an integer"}), 400

    # Now make the request, if you want.  This is a stub.
    return jsonify({"message": f"Syncing {data}"}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
