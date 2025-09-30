"""
Meta Ads (Facebook/Instagram) API connector implementation.
This module provides a connector for Meta Marketing APIs.
"""

import logging
import time
import json
from typing import Dict, List, Any, Optional, Tuple, Union
import requests
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.exceptions import FacebookRequestError

from extractors.base.api_connector import BaseAPIConnector


class MetaAdsConnector(BaseAPIConnector):
    """Meta Ads (Facebook/Instagram) API connector implementation.
    
    Attributes:
        credentials (Dict): Meta OAuth credentials
        api: Facebook Ads API instance
        access_token (str): OAuth access token
        ad_account_id (str): Meta Ad Account ID
        logger (logging.Logger): Logger for this connector
    """
    
    def __init__(self, credentials: Dict[str, Any], rate_limit_config: Optional[Dict[str, Any]] = None):
        """Initialize the Meta Ads connector.
        
        Args:
            credentials: Dictionary containing authentication credentials
                Required keys: access_token, ad_account_id, app_id, app_secret
            rate_limit_config: Optional configuration for API rate limiting
        """
        super().__init__(credentials, rate_limit_config)
        
        self.api = None
        self.access_token = credentials.get('access_token')
        self.ad_account_id = credentials.get('ad_account_id')
        self.app_id = credentials.get('app_id')
        self.app_secret = credentials.get('app_secret')
        self.last_request_time = None
        self.request_count = 0
        self.max_retries = 3
        
        # Meta API rate limits: 200 calls per hour per user, with burst allowance
        self.rate_limit_config.setdefault('requests_per_hour', 200)
        self.rate_limit_config.setdefault('min_request_interval', 0.1)  # 100ms between requests
        self.rate_limit_config.setdefault('burst_limit', 50)  # Allow bursts up to 50 requests
        
        # Ensure ad_account_id has proper format
        if self.ad_account_id and not self.ad_account_id.startswith('act_'):
            self.ad_account_id = f"act_{self.ad_account_id}"
        
    def authenticate(self) -> bool:
        """Authenticate with Meta Ads API.
        
        Returns:
            bool: True if authentication was successful, False otherwise
        """
        try:
            # Initialize Facebook Ads API
            FacebookAdsApi.init(
                app_id=self.app_id,
                app_secret=self.app_secret,
                access_token=self.access_token
            )
            
            self.api = FacebookAdsApi.get_default_api()
            self.logger.info("Successfully authenticated with Meta Ads API")
            return True
            
        except Exception as e:
            self.logger.error(f"Authentication failed: {str(e)}")
            return False
    
    def validate_connection(self) -> bool:
        """Validate the connection to Meta Ads API.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        if not self.api:
            return self.authenticate()
        
        try:
            # Try to get ad account info to validate connection
            ad_account = AdAccount(self.ad_account_id)
            account_info = ad_account.api_get(fields=['name', 'account_status'])
            
            self.logger.info(f"Connection valid for account: {account_info.get('name')}")
            return True
            
        except FacebookRequestError as e:
            if e.api_error_code() == 190:  # Invalid access token
                self.logger.error("Access token is invalid or expired")
            else:
                self.logger.error(f"Facebook API error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Connection validation error: {str(e)}")
            return False
    
    def handle_rate_limits(self):
        """Handle Meta Ads API rate limits.
        
        Meta has complex rate limiting:
        - 200 calls per hour per user
        - Burst allowance for short periods
        - Different limits for different endpoints
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
        
        # Check if we're approaching hourly limit
        if self.request_count >= self.rate_limit_config.get('requests_per_hour', 200):
            self.logger.info("Hourly rate limit reached, waiting 1 hour")
            time.sleep(3600)  # Wait 1 hour
            self.request_count = 0
    
    def fetch_data(self, 
                  object_type: str, 
                  query_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch data from Meta Ads API.
        
        Args:
            object_type: Type of object to fetch ('campaigns', 'adsets', 'ads', 'insights')
            query_params: Optional parameters for the query
                fields: List of fields to fetch
                date_preset: Date range preset (e.g., 'last_30d', 'this_month')
                time_range: Custom time range {'since': 'YYYY-MM-DD', 'until': 'YYYY-MM-DD'}
                level: Insights level ('account', 'campaign', 'adset', 'ad')
                limit: Maximum number of records
            
        Returns:
            List of dictionaries containing the fetched data
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot fetch data")
            return []
            
        query_params = query_params or {}
        
        try:
            ad_account = AdAccount(self.ad_account_id)
            records = []
            
            if object_type == 'campaigns':
                records = self._fetch_campaigns(ad_account, query_params)
            elif object_type == 'adsets':
                records = self._fetch_adsets(ad_account, query_params)
            elif object_type == 'ads':
                records = self._fetch_ads(ad_account, query_params)
            elif object_type == 'insights':
                records = self._fetch_insights(ad_account, query_params)
            else:
                self.logger.error(f"Unsupported object type: {object_type}")
                return []
            
            self.logger.info(f"Successfully fetched {len(records)} {object_type} records")
            return records
            
        except FacebookRequestError as e:
            self.logger.error(f"Facebook API error: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            return []
    
    def _fetch_campaigns(self, ad_account: AdAccount, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch campaign data."""
        fields = query_params.get('fields', [
            'id', 'name', 'status', 'objective', 'created_time', 'updated_time',
            'start_time', 'stop_time', 'budget_remaining', 'daily_budget', 'lifetime_budget'
        ])
        
        self.handle_rate_limits()
        campaigns = ad_account.get_campaigns(fields=fields)
        
        records = []
        for campaign in campaigns:
            record = dict(campaign)
            record['_object_type'] = 'campaign'
            record['_ad_account_id'] = self.ad_account_id
            record['_extracted_at'] = datetime.now().isoformat()
            records.append(record)
        
        return records
    
    def _fetch_adsets(self, ad_account: AdAccount, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch adset data."""
        fields = query_params.get('fields', [
            'id', 'name', 'status', 'campaign_id', 'created_time', 'updated_time',
            'start_time', 'end_time', 'daily_budget', 'lifetime_budget', 'bid_strategy',
            'optimization_goal', 'targeting'
        ])
        
        self.handle_rate_limits()
        adsets = ad_account.get_ad_sets(fields=fields)
        
        records = []
        for adset in adsets:
            record = dict(adset)
            record['_object_type'] = 'adset'
            record['_ad_account_id'] = self.ad_account_id
            record['_extracted_at'] = datetime.now().isoformat()
            records.append(record)
        
        return records
    
    def _fetch_ads(self, ad_account: AdAccount, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch ad data."""
        fields = query_params.get('fields', [
            'id', 'name', 'status', 'campaign_id', 'adset_id', 'created_time', 'updated_time',
            'creative', 'tracking_specs', 'conversion_specs'
        ])
        
        self.handle_rate_limits()
        ads = ad_account.get_ads(fields=fields)
        
        records = []
        for ad in ads:
            record = dict(ad)
            record['_object_type'] = 'ad'
            record['_ad_account_id'] = self.ad_account_id
            record['_extracted_at'] = datetime.now().isoformat()
            records.append(record)
        
        return records
    
    def _fetch_insights(self, ad_account: AdAccount, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch insights (performance) data."""
        fields = query_params.get('fields', [
            'impressions', 'clicks', 'spend', 'reach', 'frequency', 'cpm', 'cpc', 'ctr',
            'conversions', 'conversion_rate_ranking', 'quality_ranking', 'engagement_rate_ranking',
            'video_play_actions', 'video_p25_watched_actions', 'video_p50_watched_actions',
            'video_p75_watched_actions', 'video_p100_watched_actions'
        ])
        
        level = query_params.get('level', 'ad')
        date_preset = query_params.get('date_preset', 'last_30d')
        time_range = query_params.get('time_range')
        
        params = {
            'level': level,
            'fields': fields
        }
        
        if time_range:
            params['time_range'] = time_range
        else:
            params['date_preset'] = date_preset
        
        self.handle_rate_limits()
        insights = ad_account.get_insights(params=params)
        
        records = []
        for insight in insights:
            record = dict(insight)
            record['_object_type'] = 'insights'
            record['_level'] = level
            record['_ad_account_id'] = self.ad_account_id
            record['_extracted_at'] = datetime.now().isoformat()
            records.append(record)
        
        return records
    
    def fetch_schema(self, object_type: str) -> Dict[str, Any]:
        """Fetch the schema of a Meta Ads object.
        
        Args:
            object_type: The type of object ('campaigns', 'adsets', 'ads', 'insights')
            
        Returns:
            Dictionary containing the schema information
        """
        schemas = {
            'campaigns': {
                'fields': {
                    'id': {'type': 'string', 'description': 'Campaign ID'},
                    'name': {'type': 'string', 'description': 'Campaign name'},
                    'status': {'type': 'string', 'description': 'Campaign status'},
                    'objective': {'type': 'string', 'description': 'Campaign objective'},
                    'created_time': {'type': 'datetime', 'description': 'Creation timestamp'},
                    'updated_time': {'type': 'datetime', 'description': 'Last update timestamp'},
                    'start_time': {'type': 'datetime', 'description': 'Campaign start time'},
                    'stop_time': {'type': 'datetime', 'description': 'Campaign stop time'},
                    'budget_remaining': {'type': 'number', 'description': 'Remaining budget'},
                    'daily_budget': {'type': 'number', 'description': 'Daily budget'},
                    'lifetime_budget': {'type': 'number', 'description': 'Lifetime budget'}
                }
            },
            'adsets': {
                'fields': {
                    'id': {'type': 'string', 'description': 'Adset ID'},
                    'name': {'type': 'string', 'description': 'Adset name'},
                    'status': {'type': 'string', 'description': 'Adset status'},
                    'campaign_id': {'type': 'string', 'description': 'Parent campaign ID'},
                    'created_time': {'type': 'datetime', 'description': 'Creation timestamp'},
                    'updated_time': {'type': 'datetime', 'description': 'Last update timestamp'},
                    'optimization_goal': {'type': 'string', 'description': 'Optimization goal'},
                    'bid_strategy': {'type': 'string', 'description': 'Bidding strategy'},
                    'targeting': {'type': 'object', 'description': 'Targeting criteria'}
                }
            },
            'ads': {
                'fields': {
                    'id': {'type': 'string', 'description': 'Ad ID'},
                    'name': {'type': 'string', 'description': 'Ad name'},
                    'status': {'type': 'string', 'description': 'Ad status'},
                    'campaign_id': {'type': 'string', 'description': 'Parent campaign ID'},
                    'adset_id': {'type': 'string', 'description': 'Parent adset ID'},
                    'created_time': {'type': 'datetime', 'description': 'Creation timestamp'},
                    'creative': {'type': 'object', 'description': 'Ad creative information'}
                }
            },
            'insights': {
                'fields': {
                    'impressions': {'type': 'number', 'description': 'Number of impressions'},
                    'clicks': {'type': 'number', 'description': 'Number of clicks'},
                    'spend': {'type': 'number', 'description': 'Amount spent'},
                    'reach': {'type': 'number', 'description': 'Number of people reached'},
                    'cpm': {'type': 'number', 'description': 'Cost per thousand impressions'},
                    'cpc': {'type': 'number', 'description': 'Cost per click'},
                    'ctr': {'type': 'number', 'description': 'Click-through rate'},
                    'conversions': {'type': 'number', 'description': 'Number of conversions'}
                }
            }
        }
        
        return {
            'object_type': object_type,
            'ad_account_id': self.ad_account_id,
            'schema': schemas.get(object_type, {}),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_ad_accounts(self) -> List[Dict[str, Any]]:
        """Get available ad accounts for the authenticated user.
        
        Returns:
            List of ad account information
        """
        if not self.validate_connection():
            return []
        
        try:
            from facebook_business.adobjects.user import User
            
            self.handle_rate_limits()
            me = User(fbid='me')
            ad_accounts = me.get_ad_accounts(fields=['id', 'name', 'account_status', 'currency', 'timezone_name'])
            
            accounts = []
            for account in ad_accounts:
                accounts.append({
                    'id': account.get('id'),
                    'name': account.get('name'),
                    'status': account.get('account_status'),
                    'currency': account.get('currency'),
                    'timezone': account.get('timezone_name')
                })
            
            return accounts
            
        except Exception as e:
            self.logger.error(f"Error getting ad accounts: {str(e)}")
            return [] 