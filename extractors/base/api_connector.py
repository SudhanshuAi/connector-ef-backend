import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Generator, Callable

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)


class APIConnectorException(Exception):
    """Base exception class for API connector errors"""
    pass


class AuthenticationError(APIConnectorException):
    """Raised when authentication fails"""
    pass


class RateLimitError(APIConnectorException):
    """Raised when API rate limits are hit"""
    pass


class DataExtractionError(APIConnectorException):
    """Raised when data extraction fails"""
    pass


class TransformationError(APIConnectorException):
    """Raised when data transformation fails"""
    pass


class BaseAPIConnector(ABC):
    """Base class for all API connectors.
    
    This abstract class defines the interface that all API connectors must implement.
    It handles authentication, data fetching, and error handling for various APIs.
    
    Attributes:
        credentials (Dict): The credentials needed to authenticate with the API
        logger (logging.Logger): Logger for the connector
        rate_limit_config (Dict): Configuration for rate limiting
    """
    
    def __init__(self, credentials: Dict[str, Any], rate_limit_config: Optional[Dict[str, Any]] = None):
        """Initialize the connector with credentials and rate limit configuration.
        
        Args:
            credentials: Dictionary containing authentication credentials
            rate_limit_config: Optional configuration for API rate limiting
        """
        self.credentials = credentials
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self.rate_limit_config = rate_limit_config or {}
        self.session = None
        
    @abstractmethod
    def authenticate(self) -> bool:
        """Authenticate with the API using provided credentials.
        
        Returns:
            bool: True if authentication was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """Validate the connection to the API.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        pass
    
    @abstractmethod
    def fetch_data(self, object_name: str, query_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch data from the API.
        
        Args:
            object_name: The name of the object/entity to fetch
            query_params: Optional parameters to filter/sort the data
            
        Returns:
            List of dictionaries containing the fetched data
        """
        pass
    
    @abstractmethod
    def fetch_schema(self, object_name: str) -> Dict[str, Any]:
        """Fetch the schema of an object from the API.
        
        Args:
            object_name: The name of the object/entity
            
        Returns:
            Dictionary containing the schema information
        """
        pass
    
    def handle_rate_limits(self):
        """Handle API rate limits based on configuration.
        
        This method should be called before making API requests to avoid
        hitting rate limits. It can implement delays or backoff strategies.
        """
        # Default implementation can be overridden by specific connectors
        pass
    
    def close(self):
        """Close any open connections or sessions."""
        if self.session:
            try:
                self.session.close()
            except Exception as e:
                self.logger.error(f"Error closing session: {str(e)}")
    
    def __enter__(self):
        """Context manager entry point."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point."""
        self.close()


class APIConnector(ABC):
    """
    Abstract base class for API connectors.
    Provides a common interface for all API connectors with
    methods for authentication, data extraction, and transformation.
    """

    def __init__(self, credentials: Dict[str, str], config: Dict[str, Any]):
        """
        Initialize the API connector.
        Args:
            credentials: Dictionary containing authentication credentials
            config: Dictionary containing configuration parameters
        """
        self.credentials = credentials
        self.config = config
        self.client = None
        self.authenticated = False
        self.last_request_time = None
        self.metrics = {
            "rows_processed": 0,
            "extraction_start_time": None,
            "extraction_end_time": None,
            "api_calls": 0,
            "retries": 0
        }
        # Configure logging
        self._setup_logging()

    def _setup_logging(self):
        """Set up logging with appropriate handlers and formatters"""
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(self.config.get("log_level", logging.INFO))

    @abstractmethod
    def authenticate(self) -> bool:
        """
        Authenticate with the API and initialize the client.
        Returns:
            bool: True if authentication successful, False otherwise
        """
        pass

    @abstractmethod
    def is_token_valid(self) -> bool:
        """
        Check if the current authentication token is valid.
        Returns:
            bool: True if token is valid, False otherwise
        """
        pass

    @abstractmethod
    def refresh_token(self) -> bool:
        """
        Refresh the authentication token.
        Returns:
            bool: True if token refresh successful, False otherwise
        """
        pass

    def ensure_authenticated(self):
        """
        Ensure the client is authenticated, refreshing token if needed.
        Raises:
            AuthenticationError: If authentication fails
        """
        if not self.authenticated or not self.is_token_valid():
            logger.info("Authentication required")
            if self.authenticated and not self.is_token_valid():
                logger.info("Token expired, attempting refresh")
                success = self.refresh_token()
                if not success:
                    logger.warning("Token refresh failed, attempting full authentication")
                    success = self.authenticate()
            else:
                success = self.authenticate()
            if not success:
                raise AuthenticationError("Failed to authenticate with API")
            logger.info("Authentication successful")

    @abstractmethod
    def get_available_objects(self) -> List[str]:
        """
        Get list of available objects/entities from the API.
        Returns:
            List[str]: List of object names
        """
        pass

    @abstractmethod
    def get_object_fields(self, object_name: str) -> List[str]:
        """
        Get list of available fields for an object.
        Args:
            object_name: Name of the object
        Returns:
            List[str]: List of field names
        """
        pass

    def rate_limit_handler(self, wait_time: Optional[int] = None):
        """
        Handle rate limiting by waiting an appropriate amount of time.
        Args:
            wait_time: Optional time to wait in seconds
        """
        if wait_time is None:
            wait_time = self.config.get("default_rate_limit_wait", 60)
        logger.warning(f"Rate limit hit. Waiting {wait_time} seconds before retrying.")
        time.sleep(wait_time)

    @abstractmethod
    def extract_data(self,
                     object_name: str,
                     fields: Optional[List[str]] = None,
                     filters: Optional[Dict[str, Any]] = None,
                     batch_size: Optional[int] = None) -> Generator[List[Dict], None, None]:
        """
        Extract data from the API with batching support.
        Args:
            object_name: Name of the object to extract
            fields: Optional list of fields to extract (None = all fields)
            filters: Optional filters to apply
            batch_size: Size of batches to process
        Returns:
            Generator yielding batches of records
        """
        pass

    def transform_data(self, records: List[Dict]) -> pd.DataFrame:
        """
        Transform API records into a standardized DataFrame.
        Args:
            records: List of record dictionaries
        Returns
            pd.DataFrame: Transformed data
        """
        try:
            # Default implementation - override as needed
            df = pd.DataFrame(records)
            # Clean column names
            df.columns = [col.lower().replace(' ', '_') for col in df.columns]
            return df
        except Exception as e:
            logger.error(f"Error transforming data: {str(e)}")
            raise TransformationError(f"Failed to transform data: {str(e)}")

    def extract_and_transform(self,
                              object_name: str,
                              fields: Optional[List[str]] = None,
                              filters: Optional[Dict[str, Any]] = None,
                              incremental_field: Optional[str] = None,
                              last_sync_time: Optional[datetime] = None,
                              batch_size: int = 1000) -> pd.DataFrame:
        """
        Extract and transform data in memory-efficient batches.
        Args:
            object_name: Object to extract
            fields: Fields to include
            filters: Additional filters
            incremental_field: Field to use for incremental extraction
            last_sync_time: Last sync timestamp for incremental extraction
            batch_size: Size of each batch
        Returns:
            pd.DataFrame: Combined and transformed data
        """
        logger.info(f"Starting extraction of {object_name}")
        combined_df = None
        self.metrics["extraction_start_time"] = datetime.now()
        self.metrics["rows_processed"] = 0

        # Apply incremental filter if provided
        if incremental_field and last_sync_time:
            if not filters:
                filters = {}
            filters[incremental_field] = {"gt": last_sync_time}
            logger.info(f"Performing incremental extraction from {last_sync_time}")

        try:
            # Process data in batches
            for batch_num, batch in enumerate(self.extract_data(
                    object_name, fields, filters, batch_size)):
                logger.info(f"Processing batch {batch_num+1} with {len(batch)} records")
                # Transform batch
                batch_df = self.transform_data(batch)
                # Combine with existing results
                if combined_df is None:
                    combined_df = batch_df
                else:
                    combined_df = pd.concat([combined_df, batch_df], ignore_index=True)
                self.metrics["rows_processed"] += len(batch)

            logger.info(f"Completed extraction of {object_name}: {self.metrics['rows_processed']} rows processed")

            # Return empty DataFrame if no data found
            if combined_df is None:
                logger.info(f"No data found for {object_name}")
                return pd.DataFrame()
            return combined_df
        except Exception as e:
            logger.error(f"Error during extraction and transformation: {str(e)}")
            raise
        finally:
            self.metrics["extraction_end_time"] = datetime.now()
            self._log_metrics()

    def _log_metrics(self):
        """Log extraction metrics"""
        if self.metrics["extraction_start_time"] and self.metrics["extraction_end_time"]:
            duration = (self.metrics["extraction_end_time"] - self.metrics["extraction_start_time"]).total_seconds()
            logger.info(f"Extraction metrics: processed {self.metrics['rows_processed']} records "
                       f"in {duration:.2f} seconds "
                       f"({self.metrics['rows_processed'] / duration:.2f} records/sec if "
                       f"duration > 0 else 0)")
            logger.info(f"API calls: {self.metrics['api_calls']}, Retries: {self.metrics['retries']}")