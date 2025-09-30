"""
Google Ads API connector implementation.
This module provides a connector for Google Ads APIs.
"""

import logging
import time
import json
from typing import Dict, List, Any, Optional, Tuple, Union
import requests
from datetime import datetime, timedelta
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from extractors.base.api_connector import BaseAPIConnector


class GoogleAdsConnector(BaseAPIConnector):
    """Google Ads API connector implementation.
    
    Attributes:
        credentials (Dict): Google OAuth credentials
        client: Google Ads API client
        customer_id (str): Google Ads customer ID
        logger (logging.Logger): Logger for this connector
    """
    
    def __init__(self, credentials: Dict[str, Any], rate_limit_config: Optional[Dict[str, Any]] = None):
        """Initialize the Google Ads connector.
        
        Args:
            credentials: Dictionary containing authentication credentials
                Required keys: client_id, client_secret, refresh_token, developer_token, customer_id
            rate_limit_config: Optional configuration for API rate limiting
        """
        super().__init__(credentials, rate_limit_config)
        
        self.client = None
        self.customer_id = credentials.get('customer_id')
        self.developer_token = credentials.get('developer_token')
        self.client_id = credentials.get('client_id')
        self.client_secret = credentials.get('client_secret')
        self.refresh_token = credentials.get('refresh_token')
        self.access_token = credentials.get('access_token')
        self.last_request_time = None
        self.request_count = 0
        self.max_retries = 3
        
        # Google Ads API rate limits: 15,000 operations per day per developer token
        self.rate_limit_config.setdefault('operations_per_day', 15000)
        self.rate_limit_config.setdefault('min_request_interval', 0.1)  # 100ms between requests
        
        # Remove dashes from customer ID if present
        if self.customer_id:
            self.customer_id = self.customer_id.replace('-', '')
        
    def authenticate(self) -> bool:
        """Authenticate with Google Ads API.
        
        Returns:
            bool: True if authentication was successful, False otherwise
        """
        try:
            # Create Google Ads client configuration
            config = {
                'developer_token': self.developer_token,
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'refresh_token': self.refresh_token,
                'use_proto_plus': True
            }
            
            # Initialize Google Ads client
            self.client = GoogleAdsClient.load_from_dict(config)
            self.logger.info("Successfully authenticated with Google Ads API")
            return True
            
        except Exception as e:
            self.logger.error(f"Authentication failed: {str(e)}")
            return False
    
    def validate_connection(self) -> bool:
        """Validate the connection to Google Ads API.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        if not self.client:
            return self.authenticate()
        
        try:
            # Try to get customer info to validate connection
            customer_service = self.client.get_service("CustomerService")
            customer = customer_service.get_customer(
                resource_name=f"customers/{self.customer_id}"
            )
            
            self.logger.info(f"Connection valid for customer: {customer.descriptive_name}")
            return True
            
        except GoogleAdsException as e:
            self.logger.error(f"Google Ads API error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Connection validation error: {str(e)}")
            return False
    
    def handle_rate_limits(self):
        """Handle Google Ads API rate limits.
        
        Google Ads API has daily operation limits and request rate limits.
        """
        current_time = datetime.now()
        
        if self.last_request_time:
            elapsed = (current_time - self.last_request_time).total_seconds()
            min_interval = self.rate_limit_config.get('min_request_interval', 0.1)
            
            if elapsed < min_interval:
                sleep_time = min_interval - elapsed
                self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.3f} seconds")
                time.sleep(sleep_time)
        
        self.last_request_time = current_time
        self.request_count += 1
        
        # Log progress for large operations
        if self.request_count % 100 == 0:
            self.logger.info(f"Processed {self.request_count} API operations")
    
    def fetch_data(self, 
                  object_type: str, 
                  query_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch data from Google Ads API.
        
        Args:
            object_type: Type of object to fetch ('campaigns', 'ad_groups', 'ads', 'keywords', 'reports')
            query_params: Optional parameters for the query
                fields: List of fields to fetch
                date_range: Date range for reports {'start_date': 'YYYY-MM-DD', 'end_date': 'YYYY-MM-DD'}
                conditions: WHERE conditions for filtering
                limit: Maximum number of records
            
        Returns:
            List of dictionaries containing the fetched data
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot fetch data")
            return []
            
        query_params = query_params or {}
        
        try:
            records = []
            
            if object_type == 'campaigns':
                records = self._fetch_campaigns(query_params)
            elif object_type == 'ad_groups':
                records = self._fetch_ad_groups(query_params)
            elif object_type == 'ads':
                records = self._fetch_ads(query_params)
            elif object_type == 'keywords':
                records = self._fetch_keywords(query_params)
            elif object_type == 'campaign_performance':
                records = self._fetch_campaign_performance(query_params)
            elif object_type == 'ad_group_performance':
                records = self._fetch_ad_group_performance(query_params)
            elif object_type == 'keyword_performance':
                records = self._fetch_keyword_performance(query_params)
            else:
                self.logger.error(f"Unsupported object type: {object_type}")
                return []
            
            self.logger.info(f"Successfully fetched {len(records)} {object_type} records")
            return records
            
        except GoogleAdsException as e:
            self.logger.error(f"Google Ads API error: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            return []
    
    def _fetch_campaigns(self, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch campaign data."""
        fields = query_params.get('fields', [
            'campaign.id', 'campaign.name', 'campaign.status', 'campaign.advertising_channel_type',
            'campaign.start_date', 'campaign.end_date', 'campaign.campaign_budget'
        ])
        
        query = f"""
            SELECT {', '.join(fields)}
            FROM campaign
            WHERE campaign.status != 'REMOVED'
        """
        
        conditions = query_params.get('conditions')
        if conditions:
            query += f" AND {conditions}"
        
        return self._execute_query(query)
    
    def _fetch_ad_groups(self, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch ad group data."""
        fields = query_params.get('fields', [
            'ad_group.id', 'ad_group.name', 'ad_group.status', 'ad_group.type',
            'campaign.id', 'campaign.name'
        ])
        
        query = f"""
            SELECT {', '.join(fields)}
            FROM ad_group
            WHERE ad_group.status != 'REMOVED'
        """
        
        conditions = query_params.get('conditions')
        if conditions:
            query += f" AND {conditions}"
        
        return self._execute_query(query)
    
    def _fetch_ads(self, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch ad data."""
        fields = query_params.get('fields', [
            'ad_group_ad.ad.id', 'ad_group_ad.ad.name', 'ad_group_ad.status',
            'ad_group_ad.ad.type', 'ad_group.id', 'campaign.id'
        ])
        
        query = f"""
            SELECT {', '.join(fields)}
            FROM ad_group_ad
            WHERE ad_group_ad.status != 'REMOVED'
        """
        
        conditions = query_params.get('conditions')
        if conditions:
            query += f" AND {conditions}"
        
        return self._execute_query(query)
    
    def _fetch_keywords(self, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch keyword data."""
        fields = query_params.get('fields', [
            'ad_group_criterion.criterion_id', 'ad_group_criterion.keyword.text',
            'ad_group_criterion.keyword.match_type', 'ad_group_criterion.status',
            'ad_group.id', 'campaign.id'
        ])
        
        query = f"""
            SELECT {', '.join(fields)}
            FROM keyword_view
            WHERE ad_group_criterion.status != 'REMOVED'
        """
        
        conditions = query_params.get('conditions')
        if conditions:
            query += f" AND {conditions}"
        
        return self._execute_query(query)
    
    def _fetch_campaign_performance(self, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch campaign performance data."""
        fields = query_params.get('fields', [
            'campaign.id', 'campaign.name', 'segments.date',
            'metrics.impressions', 'metrics.clicks', 'metrics.cost_micros',
            'metrics.conversions', 'metrics.ctr', 'metrics.average_cpc'
        ])
        
        date_range = query_params.get('date_range', {})
        start_date = date_range.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = date_range.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        query = f"""
            SELECT {', '.join(fields)}
            FROM campaign
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND campaign.status != 'REMOVED'
        """
        
        conditions = query_params.get('conditions')
        if conditions:
            query += f" AND {conditions}"
        
        return self._execute_query(query)
    
    def _fetch_ad_group_performance(self, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch ad group performance data."""
        fields = query_params.get('fields', [
            'campaign.id', 'ad_group.id', 'ad_group.name', 'segments.date',
            'metrics.impressions', 'metrics.clicks', 'metrics.cost_micros',
            'metrics.conversions', 'metrics.ctr', 'metrics.average_cpc'
        ])
        
        date_range = query_params.get('date_range', {})
        start_date = date_range.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = date_range.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        query = f"""
            SELECT {', '.join(fields)}
            FROM ad_group
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND ad_group.status != 'REMOVED'
        """
        
        conditions = query_params.get('conditions')
        if conditions:
            query += f" AND {conditions}"
        
        return self._execute_query(query)
    
    def _fetch_keyword_performance(self, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch keyword performance data."""
        fields = query_params.get('fields', [
            'campaign.id', 'ad_group.id', 'ad_group_criterion.criterion_id',
            'ad_group_criterion.keyword.text', 'segments.date',
            'metrics.impressions', 'metrics.clicks', 'metrics.cost_micros',
            'metrics.conversions', 'metrics.ctr', 'metrics.average_cpc'
        ])
        
        date_range = query_params.get('date_range', {})
        start_date = date_range.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = date_range.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        query = f"""
            SELECT {', '.join(fields)}
            FROM keyword_view
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND ad_group_criterion.status != 'REMOVED'
        """
        
        conditions = query_params.get('conditions')
        if conditions:
            query += f" AND {conditions}"
        
        return self._execute_query(query)
    
    def _execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a Google Ads Query Language (GAQL) query."""
        self.handle_rate_limits()
        
        ga_service = self.client.get_service("GoogleAdsService")
        
        try:
            response = ga_service.search(
                customer_id=self.customer_id,
                query=query
            )
            
            records = []
            for row in response:
                record = self._convert_row_to_dict(row)
                record['_customer_id'] = self.customer_id
                record['_extracted_at'] = datetime.now().isoformat()
                records.append(record)
            
            return records
            
        except GoogleAdsException as e:
            self.logger.error(f"Query execution failed: {e}")
            raise
    
    def _convert_row_to_dict(self, row) -> Dict[str, Any]:
        """Convert a Google Ads API row to a dictionary."""
        record = {}
        
        # Handle different field types
        for field_name in dir(row):
            if field_name.startswith('_'):
                continue
                
            field_value = getattr(row, field_name)
            
            # Skip empty proto messages
            if hasattr(field_value, 'DESCRIPTOR') and not field_value.ListFields():
                continue
            
            # Convert proto messages to dictionaries
            if hasattr(field_value, 'DESCRIPTOR'):
                record[field_name] = self._proto_to_dict(field_value)
            else:
                record[field_name] = field_value
        
        return record
    
    def _proto_to_dict(self, proto_obj) -> Dict[str, Any]:
        """Convert a protobuf object to a dictionary."""
        result = {}
        
        for field, value in proto_obj.ListFields():
            if field.type == field.TYPE_MESSAGE:
                if field.label == field.LABEL_REPEATED:
                    result[field.name] = [self._proto_to_dict(item) for item in value]
                else:
                    result[field.name] = self._proto_to_dict(value)
            else:
                if field.label == field.LABEL_REPEATED:
                    result[field.name] = list(value)
                else:
                    result[field.name] = value
        
        return result
    
    def fetch_schema(self, object_type: str) -> Dict[str, Any]:
        """Fetch the schema of a Google Ads object.
        
        Args:
            object_type: The type of object
            
        Returns:
            Dictionary containing the schema information
        """
        schemas = {
            'campaigns': {
                'fields': {
                    'campaign.id': {'type': 'integer', 'description': 'Campaign ID'},
                    'campaign.name': {'type': 'string', 'description': 'Campaign name'},
                    'campaign.status': {'type': 'string', 'description': 'Campaign status'},
                    'campaign.advertising_channel_type': {'type': 'string', 'description': 'Channel type'},
                    'campaign.start_date': {'type': 'date', 'description': 'Start date'},
                    'campaign.end_date': {'type': 'date', 'description': 'End date'}
                }
            },
            'campaign_performance': {
                'fields': {
                    'campaign.id': {'type': 'integer', 'description': 'Campaign ID'},
                    'segments.date': {'type': 'date', 'description': 'Date'},
                    'metrics.impressions': {'type': 'integer', 'description': 'Impressions'},
                    'metrics.clicks': {'type': 'integer', 'description': 'Clicks'},
                    'metrics.cost_micros': {'type': 'integer', 'description': 'Cost in micros'},
                    'metrics.conversions': {'type': 'number', 'description': 'Conversions'},
                    'metrics.ctr': {'type': 'number', 'description': 'Click-through rate'},
                    'metrics.average_cpc': {'type': 'number', 'description': 'Average cost per click'}
                }
            }
        }
        
        return {
            'object_type': object_type,
            'customer_id': self.customer_id,
            'schema': schemas.get(object_type, {}),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_accessible_customers(self) -> List[Dict[str, Any]]:
        """Get accessible customer accounts.
        
        Returns:
            List of customer account information
        """
        if not self.validate_connection():
            return []
        
        try:
            customer_service = self.client.get_service("CustomerService")
            accessible_customers = customer_service.list_accessible_customers()
            
            customers = []
            for customer_resource in accessible_customers.resource_names:
                customer_id = customer_resource.split('/')[-1]
                
                try:
                    customer = customer_service.get_customer(resource_name=customer_resource)
                    customers.append({
                        'customer_id': customer_id,
                        'descriptive_name': customer.descriptive_name,
                        'currency_code': customer.currency_code,
                        'time_zone': customer.time_zone,
                        'manager': customer.manager
                    })
                except Exception as e:
                    self.logger.warning(f"Could not get details for customer {customer_id}: {e}")
            
            return customers
            
        except Exception as e:
            self.logger.error(f"Error getting accessible customers: {str(e)}")
            return [] 