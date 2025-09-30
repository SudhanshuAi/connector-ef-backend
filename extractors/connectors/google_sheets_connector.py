"""
Google Sheets API connector implementation.
This module provides a connector for Google Sheets APIs.
"""

import logging
import time
import json
from typing import Dict, List, Any, Optional, Tuple, Union
import requests
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from extractors.base.api_connector import BaseAPIConnector


class GoogleSheetsConnector(BaseAPIConnector):
    """Google Sheets API connector implementation.
    
    Attributes:
        credentials (Dict): Google OAuth credentials
        service: Google Sheets API service object
        access_token (str): OAuth access token
        refresh_token (str): OAuth refresh token
        logger (logging.Logger): Logger for this connector
    """
    
    def __init__(self, credentials: Dict[str, Any], rate_limit_config: Optional[Dict[str, Any]] = None):
        """Initialize the Google Sheets connector.
        
        Args:
            credentials: Dictionary containing authentication credentials
                Required keys: client_id, client_secret, access_token, refresh_token
            rate_limit_config: Optional configuration for API rate limiting
        """
        super().__init__(credentials, rate_limit_config)
        
        self.service = None
        self.access_token = credentials.get('access_token')
        self.refresh_token = credentials.get('refresh_token')
        self.client_id = credentials.get('client_id')
        self.client_secret = credentials.get('client_secret')
        self.last_request_time = None
        self.request_count = 0
        self.max_retries = 3
        
        # Google Sheets API has a quota of 100 requests per 100 seconds per user
        self.rate_limit_config.setdefault('requests_per_100_seconds', 100)
        self.rate_limit_config.setdefault('min_request_interval', 1.0)  # 1 second between requests
        
    def authenticate(self) -> bool:
        """Authenticate with Google Sheets API using OAuth 2.0.
        
        Returns:
            bool: True if authentication was successful, False otherwise
        """
        try:
            # Create credentials object
            creds = Credentials(
                token=self.access_token,
                refresh_token=self.refresh_token,
                token_uri='https://oauth2.googleapis.com/token',
                client_id=self.client_id,
                client_secret=self.client_secret,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            
            # Refresh token if needed
            if creds.expired:
                creds.refresh(Request())
                self.access_token = creds.token
                self.logger.info("Access token refreshed")
            
            # Build the service
            self.service = build('sheets', 'v4', credentials=creds)
            self.logger.info("Successfully authenticated with Google Sheets API")
            return True
            
        except Exception as e:
            self.logger.error(f"Authentication failed: {str(e)}")
            return False
    
    def validate_connection(self) -> bool:
        """Validate the connection to Google Sheets API.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        if not self.service:
            return self.authenticate()
        
        try:
            # Try to make a simple request to verify the connection
            # This will list the user's spreadsheets (requires Drive API scope)
            # For now, we'll just check if the service is available
            self.logger.info("Connection to Google Sheets API is valid")
            return True
            
        except Exception as e:
            self.logger.error(f"Connection validation error: {str(e)}")
            return False
    
    def handle_rate_limits(self):
        """Handle Google Sheets API rate limits.
        
        Google Sheets API has quotas:
        - 100 requests per 100 seconds per user
        - 500 requests per 100 seconds (total)
        """
        # Simple rate limiting based on request count and time
        current_time = datetime.now()
        
        if self.last_request_time:
            elapsed = (current_time - self.last_request_time).total_seconds()
            min_interval = self.rate_limit_config.get('min_request_interval', 1.0)
            
            if elapsed < min_interval:
                sleep_time = min_interval - elapsed
                self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        
        self.last_request_time = current_time
        self.request_count += 1
        
        # Reset counter every 100 seconds
        if self.request_count >= self.rate_limit_config.get('requests_per_100_seconds', 100):
            self.logger.info("Rate limit reached, waiting 100 seconds")
            time.sleep(100)
            self.request_count = 0
    
    def fetch_data(self, 
                  spreadsheet_id: str, 
                  query_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch data from Google Sheets.
        
        Args:
            spreadsheet_id: The ID of the Google Spreadsheet
            query_params: Optional parameters for the query
                sheet_name: Name of the sheet (default: first sheet)
                range: A1 notation range (e.g., 'A1:Z1000')
                include_headers: Whether first row contains headers (default: True)
            
        Returns:
            List of dictionaries containing the fetched data
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot fetch data")
            return []
            
        query_params = query_params or {}
        sheet_name = query_params.get('sheet_name', '')
        range_notation = query_params.get('range', '')
        include_headers = query_params.get('include_headers', True)
        
        try:
            # Get spreadsheet metadata first
            self.handle_rate_limits()
            spreadsheet = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            
            # If no sheet name specified, use the first sheet
            if not sheet_name:
                sheet_name = spreadsheet['sheets'][0]['properties']['title']
            
            # Build the range
            if range_notation:
                full_range = f"{sheet_name}!{range_notation}"
            else:
                full_range = sheet_name
            
            self.logger.debug(f"Fetching data from range: {full_range}")
            
            # Fetch the data
            self.handle_rate_limits()
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=full_range,
                valueRenderOption='UNFORMATTED_VALUE',
                dateTimeRenderOption='FORMATTED_STRING'
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                self.logger.warning("No data found in the specified range")
                return []
            
            # Convert to list of dictionaries
            records = []
            headers = None
            
            if include_headers and len(values) > 0:
                headers = values[0]
                data_rows = values[1:]
            else:
                # Generate generic headers
                max_cols = max(len(row) for row in values) if values else 0
                headers = [f"Column_{i+1}" for i in range(max_cols)]
                data_rows = values
            
            for row_index, row in enumerate(data_rows):
                record = {}
                for col_index, header in enumerate(headers):
                    # Handle rows with different lengths
                    value = row[col_index] if col_index < len(row) else None
                    record[header] = value
                
                # Add metadata
                record['_row_number'] = row_index + (2 if include_headers else 1)
                record['_sheet_name'] = sheet_name
                record['_spreadsheet_id'] = spreadsheet_id
                record['_extracted_at'] = datetime.now().isoformat()
                
                records.append(record)
            
            self.logger.info(f"Successfully fetched {len(records)} records from {sheet_name}")
            return records
            
        except HttpError as e:
            self.logger.error(f"Google Sheets API error: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            return []
    
    def fetch_schema(self, spreadsheet_id: str, sheet_name: Optional[str] = None) -> Dict[str, Any]:
        """Fetch the schema of a Google Sheet.
        
        Args:
            spreadsheet_id: The ID of the Google Spreadsheet
            sheet_name: Optional sheet name (uses first sheet if not specified)
            
        Returns:
            Dictionary containing the schema information
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot fetch schema")
            return {}
            
        try:
            # Get spreadsheet metadata
            self.handle_rate_limits()
            spreadsheet = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            
            # Find the target sheet
            target_sheet = None
            if sheet_name:
                for sheet in spreadsheet['sheets']:
                    if sheet['properties']['title'] == sheet_name:
                        target_sheet = sheet
                        break
            else:
                target_sheet = spreadsheet['sheets'][0]
                sheet_name = target_sheet['properties']['title']
            
            if not target_sheet:
                self.logger.error(f"Sheet '{sheet_name}' not found")
                return {}
            
            # Get the first row to determine headers
            self.handle_rate_limits()
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!1:1"
            ).execute()
            
            headers = result.get('values', [[]])[0] if result.get('values') else []
            
            # Build schema information
            schema = {
                'spreadsheet_id': spreadsheet_id,
                'spreadsheet_title': spreadsheet.get('properties', {}).get('title', ''),
                'sheet_name': sheet_name,
                'sheet_id': target_sheet['properties']['sheetId'],
                'row_count': target_sheet['properties']['gridProperties'].get('rowCount', 0),
                'column_count': target_sheet['properties']['gridProperties'].get('columnCount', 0),
                'headers': headers,
                'fields': {},
                'timestamp': datetime.now().isoformat()
            }
            
            # Add field information
            for i, header in enumerate(headers):
                schema['fields'][header] = {
                    'column_index': i,
                    'column_letter': self._number_to_column_letter(i + 1),
                    'type': 'string',  # Google Sheets doesn't have strict typing
                    'nullable': True
                }
            
            return schema
            
        except Exception as e:
            self.logger.error(f"Error fetching schema: {str(e)}")
            return {}
    
    def list_spreadsheets(self) -> List[Dict[str, Any]]:
        """List available spreadsheets (requires Google Drive API access).
        
        Returns:
            List of spreadsheet information
        """
        # This would require Drive API scope and implementation
        # For now, return empty list
        self.logger.warning("list_spreadsheets requires Google Drive API access")
        return []
    
    def get_spreadsheet_info(self, spreadsheet_id: str) -> Dict[str, Any]:
        """Get information about a specific spreadsheet.
        
        Args:
            spreadsheet_id: The ID of the Google Spreadsheet
            
        Returns:
            Dictionary containing spreadsheet information
        """
        if not self.validate_connection():
            return {}
        
        try:
            self.handle_rate_limits()
            spreadsheet = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            
            sheets_info = []
            for sheet in spreadsheet.get('sheets', []):
                props = sheet['properties']
                sheets_info.append({
                    'sheet_id': props['sheetId'],
                    'title': props['title'],
                    'index': props['index'],
                    'sheet_type': props.get('sheetType', 'GRID'),
                    'row_count': props.get('gridProperties', {}).get('rowCount', 0),
                    'column_count': props.get('gridProperties', {}).get('columnCount', 0)
                })
            
            return {
                'spreadsheet_id': spreadsheet_id,
                'title': spreadsheet.get('properties', {}).get('title', ''),
                'locale': spreadsheet.get('properties', {}).get('locale', ''),
                'time_zone': spreadsheet.get('properties', {}).get('timeZone', ''),
                'sheets': sheets_info
            }
            
        except Exception as e:
            self.logger.error(f"Error getting spreadsheet info: {str(e)}")
            return {}
    
    def _number_to_column_letter(self, n: int) -> str:
        """Convert column number to letter (1 -> A, 26 -> Z, 27 -> AA, etc.)."""
        result = ""
        while n > 0:
            n -= 1
            result = chr(n % 26 + ord('A')) + result
            n //= 26
        return result 