import eventlet
eventlet.monkey_patch()

import os
import json
import sys
import io
import traceback
import logging
import importlib
from typing import Dict, Type

from flask import Flask, request, jsonify, redirect
from flask_cors import CORS
from flask_sock import Sock

# --- Connector Imports (from server.py) ---
# This block safely imports connector classes. If the 'extractors' module
# isn't found or fails to import, the server will still run.
try:
    from extractors.connectors.salesforce_connector import SalesforceConnector
    from extractors.connectors.google_ads_connector import GoogleAdsConnector
    from extractors.connectors.ga4_connector import GA4Connector
    from extractors.connectors.meta_ads_connector import MetaAdsConnector
    from extractors.connectors.google_sheets_connector import GoogleSheetsConnector
    from extractors.connectors.hubspot_connector import HubspotConnector
    from extractors.base.api_connector import BaseAPIConnector
    CONNECTORS_AVAILABLE = True
except Exception as e:
    # Define a dummy base class if the import fails so the server doesn't crash
    class BaseAPIConnector: pass
    CONNECTORS_AVAILABLE = False
    IMPORT_ERROR = e # Store the error to log it later

# --- Single Flask App Initialization ---
app = Flask(__name__)
# A more secure CORS configuration for production
# It allows credentials and restricts the origin to the one specified in the environment variable.
frontend_url = os.environ.get('FRONTEND_URL', '*') # Default to wildcard for development
CORS(app, origins=[frontend_url], supports_credentials=True, methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])

sock = Sock(app) # Initialize WebSocket support

# --- Logging Configuration (from server.py) ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Connector Logic (from server.py) ---
# Connector registry is populated only if the classes were successfully imported
CONNECTOR_REGISTRY: Dict[str, Type[BaseAPIConnector]] = {
    'salesforce': SalesforceConnector,
    'google_ads': GoogleAdsConnector,
    'ga4': GA4Connector,
    'meta_ads': MetaAdsConnector,
    'google_sheets': GoogleSheetsConnector,
    'hubspot': HubspotConnector
} if CONNECTORS_AVAILABLE else {}


def get_connector(connector_type: str) -> Type[BaseAPIConnector]:
    """
    Get connector class by type. Raises ValueError if connector type not found.
    """
    if not CONNECTORS_AVAILABLE:
        raise ValueError("Connector registry is not available due to import errors.")
    connector_class = CONNECTOR_REGISTRY.get(connector_type.lower())
    if not connector_class:
        raise ValueError(f"Unsupported connector type: {connector_type}")
    return connector_class

# --- Edge Function Logic (from original app.py) ---
def execute_python_code(code: str, block_id: str) -> list:
    """
    Executes a string of Python code and captures its output and errors.
    !!! SECURITY WARNING !!! This function uses exec().
    """
    code_stdout = io.StringIO()
    code_stderr = io.StringIO()
    sys.stdout = code_stdout
    sys.stderr = code_stderr
    outputs = []
    try:
        exec(code, {})
    except Exception:
        outputs.append({
            'type': 'execute_response', 'output_type': 'error',
            'content': {
                'ename': type(sys.exc_info()[1]).__name__, 'evalue': str(sys.exc_info()[1]),
                'traceback': traceback.format_exc().splitlines()
            },
            'blockId': block_id
        })
    finally:
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__

    stdout_value = code_stdout.getvalue()
    if stdout_value:
        outputs.append({
            'type': 'execute_response', 'output_type': 'stream',
            'content': {'text': stdout_value}, 'blockId': block_id
        })
    stderr_value = code_stderr.getvalue()
    if stderr_value:
        outputs.append({
            'type': 'execute_response', 'output_type': 'error',
            'content': {'ename': 'Stderr', 'evalue': stderr_value, 'traceback': []},
            'blockId': block_id
        })
    return outputs


# --- Combined Health Check and API Routes ---

@app.route("/")
def main_health_check():
    """A comprehensive health check for the unified server."""
    return jsonify({
        "status": "healthy",
        "message": "Combined Connector and Edge Function server is running!",
        "available_connectors": list(CONNECTOR_REGISTRY.keys()),
        "connectors_loaded": CONNECTORS_AVAILABLE
    }), 200

# --- WebSocket Route for Edge Functions (from original app.py) ---
@sock.route('/ws/<path:report_id>')
def edge_function_ws(ws, report_id):
    logger.info(f"WebSocket connection established for report_id: {report_id}")
    try:
        while True:
            message_str = ws.receive(timeout=60)
            data = json.loads(message_str)
            logger.info(f"Received code execution request for {report_id}")
            if data.get('type') == 'execute_request':
                execution_results = execute_python_code(data.get('code'), data.get('blockId'))
                for result in execution_results:
                    ws.send(json.dumps(result))
    except Exception as e:
        logger.error(f"Connection closed for report_id: {report_id}. Reason: {e}")
    finally:
        logger.info(f"WebSocket for {report_id} is now fully closed.")


# --- Connector REST API Routes (from server.py) ---
@app.route('/api/connectors', methods=['GET'])
def list_connectors():
    return jsonify({"connectors": list(CONNECTOR_REGISTRY.keys())}), 200

@app.route('/api/oauth/callback/salesforce', methods=['GET'])
def salesforce_oauth_callback():
    """
    Handles the OAuth callback from Salesforce. Exchanges the authorization
    code for an access token and refresh token, then stores them securely.
    """
    auth_code = request.args.get('code')
    connection_name = request.args.get('state')

    if not auth_code or not connection_name:
        return jsonify({"error": "Missing authorization code or state"}), 400

    # These should be securely stored as environment variables on your server
    client_id = os.environ.get('SALESFORCE_CLIENT_ID')
    client_secret = os.environ.get('SALESFORCE_CLIENT_SECRET')
    # The redirect_uri for the token exchange MUST match the one used to get the code.
    # This is the URL of this very callback endpoint.
    redirect_uri = os.environ.get('SALESFORCE_CALLBACK_URL', '')

    if not all([client_id, client_secret, redirect_uri]):
        logger.error("Server is missing Salesforce OAuth environment variables.")
        return jsonify({"error": "Server configuration error."}), 500

    try:
        connector_class = get_connector('salesforce')
        # Instantiate the connector with the client credentials needed for the token exchange
        connector_instance = connector_class(credentials={'client_id': client_id, 'client_secret': client_secret})

        # Exchange the authorization code for access and refresh tokens
        token_data = connector_instance.exchange_code_for_tokens(auth_code, redirect_uri)

        # TODO: Securely save the token_data (access_token, refresh_token, instance_url)
        # to your database, associated with the user and connection_name.

        # Redirect the user back to the frontend connections page
        # The frontend can then show a success message.
        frontend_url = os.environ.get('FRONTEND_URL')
        return redirect(f"{frontend_url}/integrations?source=salesforce&status=success&conn_name={connection_name}&token_data={json.dumps(token_data)}")

    except Exception as e:
        logger.error(f"Salesforce OAuth callback failed: {str(e)}", exc_info=True)
        frontend_url = os.environ.get('FRONTEND_URL')
        # Redirect with an error status
        return redirect(f"{frontend_url}/integrations?source=salesforce&status=error&error_message={str(e)}")

@app.route('/api/connectors/refresh-token', methods=['POST'])
def refresh_token():
    """
    Refreshes an access token using a refresh token for a given connector.
    """
    data = request.get_json()
    connector_type = data.get('connector_type')
    credentials = data.get('credentials')

    if not all([connector_type, credentials, credentials.get('refresh_token')]):
        return jsonify({"error": "Missing connector_type or refresh_token"}), 400

    try:
        connector_class = get_connector(connector_type)

        # The connector needs to be instantiated with credentials required for refresh.
        # This might include client_id/secret for some providers.
        # We pass all received credentials.
        connector_instance = connector_class(credentials=credentials)

        # This method must exist on your connector classes.
        # It should take the refresh token and return new token data.
        new_token_data = connector_instance.refresh_access_token()

        # The response should include at least 'access_token' and 'expires_in'.
        # It may or may not include a new 'refresh_token'.
        return jsonify(new_token_data), 200

    except Exception as e:
        logger.error(f"Failed to refresh token for {connector_type}: {str(e)}", exc_info=True)
        return jsonify({"error": f"Token refresh failed: {str(e)}"}), 500

# (You can add the other connector endpoints like /connect, /fetch-data etc. here)
@app.route('/api/connectors/get-schema', methods=['POST'])
def get_schema():
    """
    Get the schema (list of objects/tables) for a given data source.
    """
    data = request.get_json()
    connector_type = data.get('dbtype')
    db_config = data.get('dbConfig')

    if not all([connector_type, db_config]):
        return jsonify({"error": "Missing connector type or configuration"}), 400

    try:
        connector_class = get_connector(connector_type)
        connector_instance = connector_class(credentials=db_config)
        # list_objects() should return all tables/objects.
        # The original call to fetch_schema() was incorrect as it likely expects an object_name.
        # This assumes your connector has a `list_objects` method.
        schema_data = connector_instance.list_objects()
        return jsonify({"schema": schema_data}), 200
    except Exception as e:
        logger.error(f"Failed to get schema for {connector_type}: {str(e)}", exc_info=True)
        return jsonify({"error": f"Failed to get schema: {str(e)}"}), 500

@app.route('/api/connectors/execute-query', methods=['POST'])
def execute_query():
    """
    Execute a query on a given data source.
    For Salesforce, this will be a SOQL query.
    """
    data = request.get_json()
    connector_type = data.get('dbtype')
    db_config = data.get('dbConfig')
    query = data.get('sqlstr')

    if not all([connector_type, db_config, query]):
        return jsonify({"error": "Missing connector type, configuration, or query"}), 400

    try:
        connector_class = get_connector(connector_type)
        # The credentials from your DB are passed to the connector instance
        connector_instance = connector_class(credentials=db_config)
        # Assuming the connector has a `fetch_data` method that takes a query
        results = connector_instance.fetch_data(query)
        return jsonify({"rows": results}), 200
    except Exception as e:
        logger.error(f"Failed to execute query for {connector_type}: {str(e)}", exc_info=True)
        return jsonify({"error": f"Query execution failed: {str(e)}"}), 500


# --- Server Startup Logic ---
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    
    # Log if the connectors failed to load and show the error
    if not CONNECTORS_AVAILABLE:
        logger.warning("Could not import connector modules. The REST API for connectors will not work.")
        logger.error(f"Import Error detail: {IMPORT_ERROR}")

    logger.info(f"Starting unified server on port {port}")
    if CONNECTORS_AVAILABLE:
        logger.info(f"Available connectors: {list(CONNECTOR_REGISTRY.keys())}")
    
    from eventlet import wsgi
    wsgi.server(eventlet.listen(('0.0.0.0', port)), app, log=logger)