"""
Google Analytics 4 (GA4) API connector implementation.
This module provides a connector for Google Analytics Data API.
"""

import logging
import time
import json
from typing import Dict, List, Any, Optional, Tuple, Union
import requests
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, RunRealtimeReportRequest, GetMetadataRequest,
    DateRange, Dimension, Metric, FilterExpression, Filter,
    OrderBy
)
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from extractors.base.api_connector import BaseAPIConnector


class GA4Connector(BaseAPIConnector):
    """Google Analytics 4 API connector implementation.
    
    Attributes:
        credentials (Dict): Google OAuth credentials
        client: GA4 Analytics Data API client
        property_id (str): GA4 property ID
        logger (logging.Logger): Logger for this connector
    """
    
    def __init__(self, credentials: Dict[str, Any], rate_limit_config: Optional[Dict[str, Any]] = None):
        """Initialize the GA4 connector.
        
        Args:
            credentials: Dictionary containing authentication credentials
                Required keys: client_id, client_secret, refresh_token, property_id
            rate_limit_config: Optional configuration for API rate limiting
        """
        super().__init__(credentials, rate_limit_config)
        
        self.client = None
        self.property_id = credentials.get('property_id')
        self.client_id = credentials.get('client_id')
        self.client_secret = credentials.get('client_secret')
        self.refresh_token = credentials.get('refresh_token')
        self.access_token = credentials.get('access_token')
        self.last_request_time = None
        self.request_count = 0
        self.max_retries = 3
        
        # GA4 API rate limits: 25,000 tokens per day, 250 tokens per hour
        self.rate_limit_config.setdefault('tokens_per_day', 25000)
        self.rate_limit_config.setdefault('tokens_per_hour', 250)
        self.rate_limit_config.setdefault('min_request_interval', 0.1)  # 100ms between requests
        
        # Ensure property_id has proper format
        if self.property_id and not self.property_id.startswith('properties/'):
            self.property_id = f"properties/{self.property_id}"
        
    def authenticate(self) -> bool:
        """Authenticate with GA4 API using OAuth 2.0.
        
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
                scopes=['https://www.googleapis.com/auth/analytics.readonly']
            )
            
            # Refresh token if needed
            if creds.expired:
                creds.refresh(Request())
                self.access_token = creds.token
                self.logger.info("Access token refreshed")
            
            # Initialize GA4 client
            self.client = BetaAnalyticsDataClient(credentials=creds)
            self.logger.info("Successfully authenticated with GA4 API")
            return True
            
        except Exception as e:
            self.logger.error(f"Authentication failed: {str(e)}")
            return False
    
    def validate_connection(self) -> bool:
        """Validate the connection to GA4 API.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        if not self.client:
            return self.authenticate()
        
        try:
            # Try to get metadata to validate connection
            request = GetMetadataRequest(name=f"{self.property_id}/metadata")
            metadata = self.client.get_metadata(request=request)
            
            self.logger.info(f"Connection valid for property: {self.property_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Connection validation error: {str(e)}")
            return False
    
    def handle_rate_limits(self):
        """Handle GA4 API rate limits.
        
        GA4 has token-based rate limiting:
        - 25,000 tokens per day per property
        - 250 tokens per hour per property
        - Different requests consume different amounts of tokens
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
        
        # Log progress for monitoring
        if self.request_count % 50 == 0:
            self.logger.info(f"Processed {self.request_count} GA4 API requests")
    
    def fetch_data(self, 
                  report_type: str, 
                  query_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch data from GA4 API.
        
        Args:
            report_type: Type of report ('standard', 'realtime', 'cohort', 'pivot')
            query_params: Optional parameters for the query
                dimensions: List of dimension names
                metrics: List of metric names
                date_ranges: List of date ranges [{'start_date': 'YYYY-MM-DD', 'end_date': 'YYYY-MM-DD'}]
                dimension_filter: Filter for dimensions
                metric_filter: Filter for metrics
                order_bys: List of ordering specifications
                limit: Maximum number of rows
                offset: Number of rows to skip
            
        Returns:
            List of dictionaries containing the fetched data
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot fetch data")
            return []
            
        query_params = query_params or {}
        
        try:
            records = []
            
            if report_type == 'standard':
                records = self._fetch_standard_report(query_params)
            elif report_type == 'realtime':
                records = self._fetch_realtime_report(query_params)
            elif report_type == 'cohort':
                records = self._fetch_cohort_report(query_params)
            elif report_type == 'pivot':
                records = self._fetch_pivot_report(query_params)
            else:
                self.logger.error(f"Unsupported report type: {report_type}")
                return []
            
            self.logger.info(f"Successfully fetched {len(records)} {report_type} records")
            return records
            
        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            return []
    
    def _fetch_standard_report(self, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch standard GA4 report data."""
        # Default dimensions and metrics for e-commerce
        dimensions = query_params.get('dimensions', [
            'date', 'country', 'deviceCategory', 'channelGrouping', 'source', 'medium'
        ])
        
        metrics = query_params.get('metrics', [
            'sessions', 'users', 'newUsers', 'pageviews', 'bounceRate',
            'averageSessionDuration', 'conversions', 'totalRevenue'
        ])
        
        # Date range (default: last 30 days)
        date_ranges = query_params.get('date_ranges', [{
            'start_date': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
            'end_date': datetime.now().strftime('%Y-%m-%d')
        }])
        
        # Build request
        request = RunReportRequest(
            property=self.property_id,
            dimensions=[Dimension(name=dim) for dim in dimensions],
            metrics=[Metric(name=metric) for metric in metrics],
            date_ranges=[DateRange(start_date=dr['start_date'], end_date=dr['end_date']) 
                        for dr in date_ranges],
            limit=query_params.get('limit', 10000),
            offset=query_params.get('offset', 0)
        )
        
        # Add filters if specified
        if 'dimension_filter' in query_params:
            request.dimension_filter = self._build_filter_expression(query_params['dimension_filter'])
        
        if 'metric_filter' in query_params:
            request.metric_filter = self._build_filter_expression(query_params['metric_filter'])
        
        # Add ordering if specified
        if 'order_bys' in query_params:
            request.order_bys = [self._build_order_by(order) for order in query_params['order_bys']]
        
        self.handle_rate_limits()
        response = self.client.run_report(request=request)
        
        return self._convert_report_to_records(response, 'standard')
    
    def _fetch_realtime_report(self, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch realtime GA4 report data."""
        dimensions = query_params.get('dimensions', ['country', 'deviceCategory'])
        metrics = query_params.get('metrics', ['activeUsers'])
        
        request = RunRealtimeReportRequest(
            property=self.property_id,
            dimensions=[Dimension(name=dim) for dim in dimensions],
            metrics=[Metric(name=metric) for metric in metrics],
            limit=query_params.get('limit', 10000)
        )
        
        # Add filters if specified
        if 'dimension_filter' in query_params:
            request.dimension_filter = self._build_filter_expression(query_params['dimension_filter'])
        
        if 'metric_filter' in query_params:
            request.metric_filter = self._build_filter_expression(query_params['metric_filter'])
        
        self.handle_rate_limits()
        response = self.client.run_realtime_report(request=request)
        
        return self._convert_report_to_records(response, 'realtime')
    
    def _fetch_cohort_report(self, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch cohort analysis report."""
        # This is a simplified cohort report implementation
        # Full cohort reports require more complex configuration
        self.logger.warning("Cohort reports require complex configuration - using standard report")
        return self._fetch_standard_report(query_params)
    
    def _fetch_pivot_report(self, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch pivot table report."""
        # This is a simplified pivot report implementation
        # Full pivot reports require more complex configuration
        self.logger.warning("Pivot reports require complex configuration - using standard report")
        return self._fetch_standard_report(query_params)
    
    def _build_filter_expression(self, filter_config: Dict[str, Any]) -> FilterExpression:
        """Build a filter expression from configuration."""
        # Simplified filter building - can be extended for complex filters
        if 'field_name' in filter_config and 'string_value' in filter_config:
            return FilterExpression(
                filter=Filter(
                    field_name=filter_config['field_name'],
                    string_filter=Filter.StringFilter(
                        value=filter_config['string_value'],
                        match_type=Filter.StringFilter.MatchType.EXACT
                    )
                )
            )
        return FilterExpression()
    
    def _build_order_by(self, order_config: Dict[str, Any]) -> OrderBy:
        """Build an OrderBy from configuration."""
        order_by = OrderBy()
        
        if 'dimension' in order_config:
            order_by.dimension = OrderBy.DimensionOrderBy(dimension_name=order_config['dimension'])
        elif 'metric' in order_config:
            order_by.metric = OrderBy.MetricOrderBy(metric_name=order_config['metric'])
        
        if order_config.get('desc', False):
            order_by.desc = True
        
        return order_by
    
    def _convert_report_to_records(self, response, report_type: str) -> List[Dict[str, Any]]:
        """Convert GA4 API response to list of records."""
        records = []
        
        # Get dimension and metric headers
        dimension_headers = [header.name for header in response.dimension_headers]
        metric_headers = [header.name for header in response.metric_headers]
        
        # Process each row
        for row in response.rows:
            record = {}
            
            # Add dimension values
            for i, dim_value in enumerate(row.dimension_values):
                if i < len(dimension_headers):
                    record[dimension_headers[i]] = dim_value.value
            
            # Add metric values
            for i, metric_value in enumerate(row.metric_values):
                if i < len(metric_headers):
                    record[metric_headers[i]] = metric_value.value
            
            # Add metadata
            record['_report_type'] = report_type
            record['_property_id'] = self.property_id
            record['_extracted_at'] = datetime.now().isoformat()
            
            records.append(record)
        
        return records
    
    def fetch_schema(self, object_type: str = 'metadata') -> Dict[str, Any]:
        """Fetch the schema/metadata of GA4 property.
        
        Args:
            object_type: Type of schema to fetch ('metadata', 'dimensions', 'metrics')
            
        Returns:
            Dictionary containing the schema information
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot fetch schema")
            return {}
        
        try:
            request = GetMetadataRequest(name=f"{self.property_id}/metadata")
            self.handle_rate_limits()
            metadata = self.client.get_metadata(request=request)
            
            schema = {
                'property_id': self.property_id,
                'dimensions': {},
                'metrics': {},
                'timestamp': datetime.now().isoformat()
            }
            
            # Process dimensions
            for dimension in metadata.dimensions:
                schema['dimensions'][dimension.api_name] = {
                    'display_name': dimension.ui_name,
                    'description': dimension.description,
                    'category': dimension.category,
                    'deprecated': dimension.deprecated
                }
            
            # Process metrics
            for metric in metadata.metrics:
                schema['metrics'][metric.api_name] = {
                    'display_name': metric.ui_name,
                    'description': metric.description,
                    'type': metric.type_.name,
                    'category': metric.category,
                    'deprecated': metric.deprecated
                }
            
            return schema
            
        except Exception as e:
            self.logger.error(f"Error fetching schema: {str(e)}")
            return {}
    
    def get_account_summaries(self) -> List[Dict[str, Any]]:
        """Get GA4 account summaries (requires Admin API access).
        
        Returns:
            List of account summary information
        """
        # This would require GA4 Admin API access
        # For now, return empty list with warning
        self.logger.warning("get_account_summaries requires GA4 Admin API access")
        return []
    
    def get_custom_dimensions(self) -> List[Dict[str, Any]]:
        """Get custom dimensions for the property.
        
        Returns:
            List of custom dimension information
        """
        try:
            schema = self.fetch_schema()
            custom_dimensions = []
            
            for api_name, dimension_info in schema.get('dimensions', {}).items():
                if api_name.startswith('customEvent:') or api_name.startswith('customUser:'):
                    custom_dimensions.append({
                        'api_name': api_name,
                        'display_name': dimension_info['display_name'],
                        'description': dimension_info['description'],
                        'scope': 'EVENT' if api_name.startswith('customEvent:') else 'USER'
                    })
            
            return custom_dimensions
            
        except Exception as e:
            self.logger.error(f"Error getting custom dimensions: {str(e)}")
            return []
    
    def get_custom_metrics(self) -> List[Dict[str, Any]]:
        """Get custom metrics for the property.
        
        Returns:
            List of custom metric information
        """
        try:
            schema = self.fetch_schema()
            custom_metrics = []
            
            for api_name, metric_info in schema.get('metrics', {}).items():
                if api_name.startswith('customEvent:'):
                    custom_metrics.append({
                        'api_name': api_name,
                        'display_name': metric_info['display_name'],
                        'description': metric_info['description'],
                        'type': metric_info['type']
                    })
            
            return custom_metrics
            
        except Exception as e:
            self.logger.error(f"Error getting custom metrics: {str(e)}")
            return [] 