"""
Abstract base class for data extractors.
This module defines the interface for all extractors in the framework.
"""

from abc import ABC, abstractmethod
import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union

from extractors.base.api_connector import BaseAPIConnector
from extractors.base.base_storage_manager import BaseStorageManager


class BaseExtractor(ABC):
    """Base class for all data extractors.
    
    This abstract class defines the interface that all extractors must implement.
    It orchestrates the extraction process from source to destination.
    
    Attributes:
        connector (BaseAPIConnector): The API connector to use
        storage (BaseStorageManager): The storage manager to use
        logger (logging.Logger): Logger for the extractor
        config (Dict): Extraction configuration
    """
    
    def __init__(self, 
                connector: BaseAPIConnector, 
                storage: BaseStorageManager,
                config: Dict[str, Any]):
        """Initialize the extractor with a connector and storage manager.
        
        Args:
            connector: The API connector to use for extraction
            storage: The storage manager for storing extracted data
            config: Configuration for the extraction process
        """
        self.connector = connector
        self.storage = storage
        self.config = config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        
    @abstractmethod
    def extract(self, 
               object_names: List[str],
               extraction_type: str = 'full',
               since_date: Optional[str] = None) -> Dict[str, Any]:
        """Extract data from the source system and store it.
        
        Args:
            object_names: List of object names to extract
            extraction_type: Type of extraction ('full', 'incremental')
            since_date: For incremental extraction, extract data since this date
            
        Returns:
            Dictionary with extraction results and metadata
        """
        pass
    
    @abstractmethod
    def extract_incremental(self, 
                          object_name: str,
                          since_date: str) -> Tuple[bool, Dict[str, Any]]:
        """Extract data incrementally from the specified date.
        
        Args:
            object_name: The object to extract data from
            since_date: Extract data modified since this date
            
        Returns:
            Tuple containing:
            - bool: True if extraction was successful, False otherwise
            - Dict: Metadata about the extraction
        """
        pass
    
    @abstractmethod
    def extract_full(self, 
                   object_name: str) -> Tuple[bool, Dict[str, Any]]:
        """Extract all data from the source.
        
        Args:
            object_name: The object to extract data from
            
        Returns:
            Tuple containing:
            - bool: True if extraction was successful, False otherwise
            - Dict: Metadata about the extraction
        """
        pass
    
    def validate_data_quality(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate the quality of extracted data.
        
        Args:
            data: The data to validate
            
        Returns:
            Dictionary with validation results
        """
        # Default implementation with basic checks
        results = {
            "total_records": len(data),
            "null_values": {},
            "duplicate_keys": 0,
            "validation_timestamp": datetime.now().isoformat(),
        }
        
        # Check for null values in important fields
        if data and len(data) > 0:
            important_fields = self.config.get("important_fields", [])
            for field in important_fields:
                null_count = sum(1 for record in data if field not in record or record[field] is None)
                results["null_values"][field] = null_count
        
        return results
    
    def handle_schema_changes(self, 
                             object_name: str, 
                             new_schema: Dict[str, Any]) -> Tuple[bool, str]:
        """Handle changes in the schema of an object.
        
        Args:
            object_name: The object whose schema changed
            new_schema: The new schema
            
        Returns:
            Tuple containing:
            - bool: True if handled successfully, False otherwise
            - str: Message about the handling
        """
        # Default implementation saves the schema
        try:
            schema_path = f"schemas/{object_name}_schema_{int(time.time())}.json"
            success, message = self.storage.store_data(new_schema, schema_path)
            return success, f"Schema saved to {message}"
        except Exception as e:
            self.logger.error(f"Error handling schema change: {str(e)}")
            return False, str(e)
    
    def close(self):
        """Close connectors and storage managers."""
        try:
            if hasattr(self, 'connector') and self.connector:
                self.connector.close()
            if hasattr(self, 'storage') and self.storage:
                self.storage.close()
        except Exception as e:
            self.logger.error(f"Error closing resources: {str(e)}")
    
    def __enter__(self):
        """Context manager entry point."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point."""
        self.close()
