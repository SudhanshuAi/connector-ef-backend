"""
HubSpot API connector implementation.
This module provides a connector for HubSpot APIs.
"""

import logging
import time
import json
from typing import Dict, List, Any, Optional, Tuple
import requests
from datetime import datetime

from extractors.base.api_connector import BaseAPIConnector


class HubspotConnector(BaseAPIConnector):
    """HubSpot API connector implementation.

    Attributes:
        credentials (Dict): HubSpot authentication credentials
        access_token (str): OAuth access token
        refresh_token (str): OAuth refresh token
        api_base_url (str): HubSpot API base URL
        logger (logging.Logger): Logger for this connector
    """

    def __init__(self, credentials: Optional[Dict[str, Any]] = None, rate_limit_config: Optional[Dict[str, Any]] = None):
        """Initialize the HubSpot connector.

        Args:
            credentials: Dictionary containing authentication credentials
                - `client_id`, `client_secret`
                - Optional: `access_token`, `refresh_token`, `redirect_uri`
            rate_limit_config: Optional configuration for API rate limiting
        """
        credentials = credentials or {}
        super().__init__(credentials, rate_limit_config)

        self.api_base_url = "https://api.hubapi.com"
        self.access_token = credentials.get('access_token')
        self.refresh_token = credentials.get('refresh_token')
        self.last_request_time = None
        self.session = requests.Session()
        self.request_count = 0
        self.max_retries = 3

        if self.access_token:
            self.session.headers.update({
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            })

    def exchange_code_for_tokens(self, auth_code: str, redirect_uri: str) -> Dict[str, Any]:
        """
        Exchanges an authorization code for an access token and a refresh token.

        Args:
            auth_code: The authorization code from the HubSpot callback.
            redirect_uri: The exact redirect URI used in the initial auth request.

        Returns:
            A dictionary containing the token data from HubSpot.
        """
        auth_url = f"{self.api_base_url}/oauth/v1/token"
        payload = {
            'grant_type': 'authorization_code',
            'client_id': self.credentials['client_id'],
            'client_secret': self.credentials['client_secret'],
            'redirect_uri': redirect_uri,
            'code': auth_code,
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        self.logger.info(f"Exchanging HubSpot code for tokens.")
        response = self.session.post(auth_url, data=payload, headers=headers)
        response.raise_for_status()
        token_data = response.json()

        self.access_token = token_data.get('access_token')
        self.refresh_token = token_data.get('refresh_token')
        self.session.headers.update({'Authorization': f'Bearer {self.access_token}'})

        self.logger.info("Successfully exchanged HubSpot code for tokens.")
        return token_data

    def authenticate(self) -> bool:
        """
        Authenticates with HubSpot. For HubSpot, this means ensuring we have a valid
        access token, refreshing if necessary. Direct authentication is handled
        via the OAuth flow (exchange_code_for_tokens).
        """
        return self.validate_connection()

    def refresh_access_token(self) -> bool:
        """Refresh the access token using the refresh token."""
        if not self.refresh_token:
            self.logger.error("No refresh token available to refresh the access token.")
            return False

        try:
            auth_url = f"{self.api_base_url}/oauth/v1/token"
            payload = {
                'grant_type': 'refresh_token',
                'client_id': self.credentials['client_id'],
                'client_secret': self.credentials['client_secret'],
                'refresh_token': self.refresh_token,
            }
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            response = self.session.post(auth_url, data=payload, headers=headers)
            response.raise_for_status()
            auth_data = response.json()

            self.access_token = auth_data['access_token']
            # HubSpot refresh tokens are single-use, so we must store the new one.
            self.refresh_token = auth_data.get('refresh_token', self.refresh_token)
            self.session.headers.update({'Authorization': f'Bearer {self.access_token}'})
            self.logger.info("Successfully refreshed HubSpot access token.")
            return True
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error refreshing access token: {e.response.text if e.response else str(e)}")
            return False

    def validate_connection(self) -> bool:
        """Validate the connection to HubSpot.

        Returns:
            bool: True if connection is valid, False otherwise
        """
        if not self.access_token:
            self.logger.warning("No access token present for connection validation.")
            return False

        try:
            # A simple request to get account details to verify the token.
            url = f"{self.api_base_url}/account-info/v3/details"
            response = self.session.get(url)

            if response.status_code == 200:
                self.logger.info("Connection to HubSpot is valid.")
                return True
            elif response.status_code == 401:
                self.logger.info("HubSpot access token may be expired, attempting to refresh.")
                return self.refresh_access_token()
            else:
                self.logger.error(f"Connection validation failed: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            self.logger.error(f"Connection validation error: {str(e)}")
            return False

    def handle_rate_limits(self):
        """Handle HubSpot API rate limits (basic implementation).

        HubSpot has a limit of 100 requests per 10 seconds.
        """
        # A simple delay to stay under the limit.
        time.sleep(0.11) # Sleep for 110ms between requests

    def fetch_data(self,
                   object_type: str,
                   query_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch data from HubSpot using the CRM Search API.

        Args:
            object_type: The type of HubSpot object to fetch (e.g., 'contacts', 'companies').
            query_params: Optional parameters for the search query.
                properties: List of properties to retrieve.
                limit: The maximum number of results to return in a single page.

        Returns:
            List of dictionaries containing the fetched data.
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot fetch data.")
            return []

        query_params = query_params or {}
        url = f"{self.api_base_url}/crm/v3/objects/{object_type}/search"
        all_results = []
        after = None

        # Default properties if not provided
        properties = query_params.get('properties', ["createdate", "lastmodifieddate", "hs_object_id"])

        try:
            while True:
                self.handle_rate_limits()
                payload = {
                    "properties": properties,
                    "limit": query_params.get('limit', 100)
                }
                if after:
                    payload['after'] = after

                self.logger.debug(f"Fetching HubSpot data for '{object_type}' with payload: {json.dumps(payload)}")
                response = self.session.post(url, json=payload)
                response.raise_for_status()
                data = response.json()

                results = data.get('results', [])
                # Flatten properties for easier access
                for item in results:
                    flat_item = item.get('properties', {})
                    flat_item['id'] = item.get('id')
                    all_results.append(flat_item)

                # Handle pagination
                if 'paging' in data and 'next' in data['paging']:
                    after = data['paging']['next']['after']
                else:
                    break

            self.logger.info(f"Successfully fetched {len(all_results)} records for '{object_type}'.")
            return all_results

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching data for '{object_type}': {e.response.text if e.response else str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while fetching data: {str(e)}")
            return []

    def fetch_schema(self, object_type: str) -> Dict[str, Any]:
        """Fetch the schema of a HubSpot object.

        Args:
            object_type: The name of the HubSpot object (e.g., 'contacts').

        Returns:
            Dictionary containing the schema information.
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot fetch schema.")
            return {}

        try:
            self.handle_rate_limits()
            url = f"{self.api_base_url}/crm/v3/schemas/{object_type}"
            response = self.session.get(url)
            response.raise_for_status()
            schema = response.json()

            field_info = {}
            for prop in schema.get('properties', []):
                field_info[prop['name']] = {
                    'type': prop.get('type'),
                    'label': prop.get('label'),
                    'fieldType': prop.get('fieldType'),
                }

            return {
                'name': schema.get('name'),
                'labels': schema.get('labels'),
                'primaryDisplayProperty': schema.get('primaryDisplayProperty'),
                'properties': field_info,
                'timestamp': datetime.now().isoformat()
            }

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching schema for '{object_type}': {e.response.text if e.response else str(e)}")
            return {}
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while fetching schema: {str(e)}")
            return {}

    def list_objects(self) -> Dict[str, Any]:
        """Fetch a list of all available CRM objects and their schemas from HubSpot.

        Returns:
            A dictionary structured for the frontend, containing schema and table info.
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot list objects.")
            return {}

        try:
            self.handle_rate_limits()
            url = f"{self.api_base_url}/crm/v3/schemas"
            response = self.session.get(url)
            response.raise_for_status()
            all_schemas = response.json().get('results', [])

            full_schema = {}
            for schema in all_schemas:
                object_type = schema['name']
                self.logger.debug(f"Processing schema for {object_type}")
                fields_list = []
                for prop in schema.get('properties', []):
                    fields_list.append({
                        'columnName': prop['name'],
                        'dataType': prop.get('type', 'string')
                    })
                full_schema[object_type] = fields_list

            return {'hubspot': full_schema}

        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request error while listing objects: {str(e)}")
            return {}
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while listing objects: {str(e)}")
            return {}
