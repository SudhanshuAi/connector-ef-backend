"""
Salesforce API connector implementation.
This module provides a connector for Salesforce APIs.
"""
import os

import logging
import time
import json
from typing import Dict, List, Any, Optional, Tuple, Union
import requests
from datetime import datetime, timedelta

from extractors.base.api_connector import BaseAPIConnector


class SalesforceConnector(BaseAPIConnector):
    """Salesforce API connector implementation.
    
    Attributes:
        credentials (Dict): Salesforce authentication credentials
        instance_url (str): Salesforce instance URL
        access_token (str): OAuth access token
        api_version (str): Salesforce API version
        logger (logging.Logger): Logger for this connector
    """
    
    def __init__(self, credentials: Optional[Dict[str, Any]] = None, rate_limit_config: Optional[Dict[str, Any]] = None):
        """Initialize the Salesforce connector.
        
        Args:
            credentials: Dictionary containing authentication credentials
                - For password flow: `client_id`, `client_secret`, `username`, `password`
                - For OAuth flow: `client_id`, `client_secret`, `access_token`, `refresh_token`, `instance_url`
                Optional keys: `security_token`, `api_version`, `sandbox`
            rate_limit_config: Optional configuration for API rate limiting
        """
        credentials = credentials or {}
        super().__init__(credentials, rate_limit_config)
        
        self.instance_url = credentials.get('instance_url')
        self.api_version = credentials.get('api_version', '57.0')
        self.access_token = credentials.get('access_token')
        self.refresh_token = credentials.get('refresh_token')
        self.last_request_time = None
        self.client_id = os.environ.get('SALESFORCE_CLIENT_ID')
        self.client_secret = os.environ.get('SALESFORCE_CLIENT_SECRET')
        self.session = requests.Session()
        self.request_count = 0
        self.max_retries = 3

        # Determine auth type based on provided credentials
        # if 'access_token' in self.credentials and 'refresh_token' in self.credentials:
        if 'access_token' in self.credentials:
            self.auth_type = 'token'
            self.session.headers.update({
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            })
        elif 'username' in self.credentials and 'password' in self.credentials:
            self.auth_type = 'password'
        else:
            # Allows instantiation for code exchange before full credentials are known
            self.auth_type = 'unauthenticated'

    def _get_auth_url(self) -> str:
        """Gets the correct Salesforce auth URL (production or sandbox)."""
        if self.credentials.get('sandbox', False):
            return 'https://test.salesforce.com/services/oauth2/token'
        return 'https://login.salesforce.com/services/oauth2/token'

    def exchange_code_for_tokens(self, auth_code: str, redirect_uri: str) -> Dict[str, Any]:
        """
        Exchanges an authorization code for an access token and a refresh token.

        Args:
            auth_code: The authorization code from the Salesforce callback.
            redirect_uri: The exact redirect URI used in the initial auth request.

        Returns:
            A dictionary containing the token data from Salesforce.
        """
        auth_url = self._get_auth_url()
        payload = {
            'grant_type': 'authorization_code',
            'code': auth_code,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'redirect_uri': redirect_uri,
        }
        self.logger.info(f"Exchanging code for tokens with payload: {json.dumps({k: v for k, v in payload.items() if k != 'client_secret'}, indent=2)}")
        response = self.session.post(auth_url, data=payload)
        if response.status_code != 200:
            self.logger.error(f"Token exchange failed with status {response.status_code}: {response.text}")
        response.raise_for_status()
        self.logger.info(f"Successfully exchanged code for tokens {response.json()}")
        return response.json()

    def authenticate(self) -> bool:
        """Authenticate with Salesforce using the provided credentials.
        
        This method supports password-based OAuth flow for Salesforce.
        
        Returns:
            bool: True if authentication was successful, False otherwise
        """
        try:
            if self.auth_type != 'password':
                self.logger.error("Authenticate called for non-password credentials. Use token refresh instead.")
                return self.validate_connection()

            auth_url = self._get_auth_url()
            payload = {
                'grant_type': 'password',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'username': self.credentials['username'],
                'password': self.credentials['password'],
            }
            
            # Add security token to password if provided
            if 'security_token' in self.credentials:
                payload['password'] += self.credentials['security_token']
            
            response = self.session.post(auth_url, data=payload)
            
            if response.status_code == 200:
                auth_data = response.json()
                self.access_token = auth_data['access_token']
                self.instance_url = auth_data.get('instance_url', self.instance_url)
                self.session.headers.update({
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/json'
                })
                self.logger.info("Successfully authenticated with Salesforce")
                return True
            else:
                self.logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                return False
        
        except Exception as e:
            self.logger.error(f"Authentication error: {str(e)}")
            return False

    def refresh_access_token(self) -> Dict[str, Any]:
        """Refresh the access token using the refresh token."""
        if not self.refresh_token:
            self.logger.error("No refresh token available to refresh the access token.")
            raise Exception("No refresh token available.")

        try:
            auth_url = self._get_auth_url()
            payload = {
                'grant_type': 'refresh_token',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'refresh_token': self.refresh_token,
            }
            response = self.session.post(auth_url, data=payload)
            response.raise_for_status()
            auth_data = response.json()
            self.access_token = auth_data['access_token']
            # Salesforce may issue a new refresh token, but often doesn't. Handle if it does.
            self.refresh_token = auth_data.get('refresh_token', self.refresh_token)
            self.session.headers.update({'Authorization': f'Bearer {self.access_token}'})
            self.logger.info("Successfully refreshed Salesforce access token.")
            return auth_data
        except Exception as e:
            self.logger.error(f"Error refreshing access token: {str(e)}")
            raise e
    
    def validate_connection(self) -> bool:
        """Validate the connection to Salesforce.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        if not self.access_token:
            if self.auth_type == 'password':
                return self.authenticate()
            self.logger.warning("No access token present for connection validation.")
            return False
        
        try:
            # Try to make a simple request to verify the connection
            url = f"{self.instance_url}/services/data/v{self.api_version}/sobjects"
            response = self.session.get(url)
            
            if response.status_code == 200:
                self.logger.info("Connection to Salesforce is valid")
                return True
            elif response.status_code == 401:
                self.logger.info("Access token may be expired, attempting to refresh/re-authenticate.")
                if self.auth_type == 'token':
                    return self.refresh_access_token()
                elif self.auth_type == 'password':
                    return self.authenticate()
                else:
                    return False
            else:
                self.logger.error(f"Connection validation failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Connection validation error: {str(e)}")
            return False
    
    def handle_rate_limits(self):
        """Handle Salesforce API rate limits.
        
        Salesforce enforces various API limits, this method implements
        a simple delay mechanism to avoid hitting those limits.
        """
        # Simple rate limiting based on request count
        if self.request_count >= 100:  # Reset after 100 requests
            time.sleep(5)  # Wait 5 seconds
            self.request_count = 0
            return
            
        # Ensure at least 100ms between requests
        if self.last_request_time:
            elapsed = datetime.now() - self.last_request_time
            if elapsed.total_seconds() < 0.1:
                time.sleep(0.1 - elapsed.total_seconds())
                
        self.last_request_time = datetime.now()
        self.request_count += 1
    
    def fetch_data(self, 
                  query_or_object_name: str, 
                  query_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch data from Salesforce using SOQL queries.
        
        Args:
            query_or_object_name: A full SOQL query string or the name of a Salesforce object.
            query_params: Optional parameters for the SOQL query
                fields: List of fields to fetch
                where: SOQL WHERE clause
                limit: Maximum number of records
                order_by: SOQL ORDER BY clause

        Returns:
            List of dictionaries containing the fetched data
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot fetch data")
            return []

        query: str
        # Check if a full query is provided or if we need to build one.
        # A simple heuristic is to check for "SELECT" and "FROM" keywords.
        if 'select' in query_or_object_name.lower() and 'from' in query_or_object_name.lower():
            query = query_or_object_name
        else:
            # Build the query from object_name and query_params
            object_name = query_or_object_name
            query_params = query_params or {}
            fields = query_params.get('fields', ['Id', 'Name', 'CreatedDate', 'LastModifiedDate'])
            where_clause = query_params.get('where', '')
            limit_clause = f"LIMIT {query_params.get('limit', 2000)}" if 'limit' in query_params else ""
            order_by = query_params.get('order_by', '')
            
            # Build SOQL query
            fields_str = ', '.join(fields)
            query = f"SELECT {fields_str} FROM {object_name}"
            
            if where_clause:
                query += f" WHERE {where_clause}"
                
            if order_by:
                query += f" ORDER BY {order_by}"
                
            if limit_clause:
                query += f" {limit_clause}"
            
        self.logger.debug(f"SOQL Query: {query}")
        
        try:
            # Execute query
            self.handle_rate_limits()
            url = f"{self.instance_url}/services/data/v{self.api_version}/query"
            response = self.session.get(url, params={'q': query})
            
            if response.status_code == 200:
                data = response.json()
                records = data.get('records', [])
                
                # Handle pagination for large result sets
                next_url = data.get('nextRecordsUrl')
                while next_url:
                    self.handle_rate_limits()
                    self.logger.debug(f"Fetching next batch from: {next_url}")
                    next_url_full = f"{self.instance_url}{next_url}"
                    response = self.session.get(next_url_full)
                    
                    if response.status_code == 200:
                        data = response.json()
                        records.extend(data.get('records', []))
                        next_url = data.get('nextRecordsUrl')
                    else:
                        self.logger.error(f"Error fetching next batch: {response.status_code} - {response.text}")
                        break
                
                # Remove Salesforce metadata attributes
                for record in records:
                    if 'attributes' in record:
                        del record['attributes']
                
                self.logger.info(f"Successfully fetched {len(records)} records.")
                return records
            else:
                self.logger.error(f"Error fetching data: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            return []
    
    def fetch_schema(self, object_name: str) -> Dict[str, Any]:
        """Fetch the schema of a Salesforce object.
        
        Args:
            object_name: The name of the Salesforce object
            
        Returns:
            Dictionary containing the schema information
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot fetch schema")
            return {}
            
        try:
            # Get object description
            self.handle_rate_limits()
            url = f"{self.instance_url}/services/data/v{self.api_version}/sobjects/{object_name}/describe"
            response = self.session.get(url)
            
            if response.status_code == 200:
                schema = response.json()
                
                # Extract relevant schema information
                fields = schema.get('fields', [])
                field_info = {}
                
                for field in fields:
                    field_info[field['name']] = {
                        'type': field['type'],
                        'label': field['label'],
                        'length': field.get('length'),
                        'nillable': field.get('nillable', True),
                        'createable': field.get('createable', False),
                        'updateable': field.get('updateable', False),
                    }
                
                return {
                    'name': schema.get('name'),
                    'label': schema.get('label'),
                    'fields': field_info,
                    'timestamp': datetime.now().isoformat()
                }
            else:
                self.logger.error(f"Error fetching schema: {response.status_code} - {response.text}")
                return {}
                
        except Exception as e:
            self.logger.error(f"Error fetching schema: {str(e)}")
            return {}

    def list_objects(self) -> Dict[str, Any]:
        """Fetch a list of all available SObjects and their schemas from Salesforce.

        Returns:
            A dictionary structured for the frontend, containing schema and table info.
            Example: {'salesforce': {'Account': [{'columnName': 'Id', 'dataType': 'id'}, ...], ...}}
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot list objects")
            return {}

        try:
            # 1. Get the list of all SObjects
            self.handle_rate_limits()
            list_url = f"{self.instance_url}/services/data/v{self.api_version}/sobjects"
            response = self.session.get(list_url)
            response.raise_for_status()
            all_objects_data = response.json()

            sobjects = all_objects_data.get('sobjects', [])
            # We only want queryable objects that are not custom settings or events.
            object_names = [
                obj['name'] for obj in sobjects 
                if obj.get('queryable') and not obj.get('customSetting') and not obj['name'].endswith(('__e', '__c'))
            ]

            self.logger.info(f"Found {len(object_names)} queryable SObjects to describe.")

            full_schema = {}
            # Limit to a reasonable number for performance, e.g., the first 100.
            for object_name in object_names[:100]:
                self.logger.debug(f"Fetching schema for {object_name}")
                object_schema = self.fetch_schema(object_name)
                if object_schema and 'fields' in object_schema:
                    fields_list = []
                    for field_name, field_details in object_schema['fields'].items():
                        fields_list.append({
                            'columnName': field_name,
                            'dataType': field_details.get('type', 'string')
                        })
                    full_schema[object_name] = fields_list
                else:
                    self.logger.warning(f"Could not retrieve schema for {object_name}")
            
            # The frontend expects a top-level schema name.
            return {'salesforce': full_schema}

        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request error while listing objects: {str(e)}")
            return {}
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while listing objects: {str(e)}")
            return {}