"""
Abstract base class for storage managers.
This module defines the interface for all storage managers in the extraction framework.
"""

from abc import ABC, abstractmethod
import logging
from typing import Dict, List, Any, Optional, BinaryIO, Union, Tuple


class BaseStorageManager(ABC):
    """Base class for all storage managers.
    
    This abstract class defines the interface that all storage managers must implement.
    It handles storing and retrieving data to/from various storage systems.
    
    Attributes:
        config (Dict): Configuration for the storage system
        logger (logging.Logger): Logger for the storage manager
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the storage manager with configuration.
        
        Args:
            config: Dictionary containing storage system configuration
        """
        self.config = config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        
    @abstractmethod
    def connect(self) -> bool:
        """Connect to the storage system.
        
        Returns:
            bool: True if connection was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def validate_storage(self) -> bool:
        """Validate the connection to the storage system.
        
        Returns:
            bool: True if storage system is accessible, False otherwise
        """
        pass
    
    @abstractmethod
    def store_data(self, 
                  data: Union[List[Dict[str, Any]], str, bytes], 
                  path: str,
                  metadata: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
        """Store data to the storage system.
        
        Args:
            data: The data to store (can be list of dicts, string, or bytes)
            path: The path where the data should be stored
            metadata: Optional metadata to store with the data
            
        Returns:
            Tuple containing:
            - bool: True if storing was successful, False otherwise
            - str: Path/identifier where the data was stored or error message
        """
        pass
    
    @abstractmethod
    def retrieve_data(self, 
                     path: str,
                     as_type: str = 'dict') -> Tuple[bool, Union[List[Dict[str, Any]], str, bytes, None]]:
        """Retrieve data from the storage system.
        
        Args:
            path: The path from where to retrieve the data
            as_type: The return type ('dict', 'string', 'bytes')
            
        Returns:
            Tuple containing:
            - bool: True if retrieval was successful, False otherwise
            - Union[List[Dict], str, bytes, None]: The retrieved data or None if failed
        """
        pass
    
    @abstractmethod
    def list_files(self, path: str) -> List[str]:
        """List files in a specific path.
        
        Args:
            path: The path to list files from
            
        Returns:
            List of file paths
        """
        pass
    
    def handle_partitioning(self, data: List[Dict[str, Any]], partition_size: int = 1000) -> List[List[Dict[str, Any]]]:
        """Partition data for efficient storage.
        
        Args:
            data: The data to partition
            partition_size: Maximum number of records per partition
            
        Returns:
            List of partitioned data
        """
        return [data[i:i+partition_size] for i in range(0, len(data), partition_size)]
    
    def close(self):
        """Close any open connections."""
        pass
    
    def __enter__(self):
        """Context manager entry point."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point."""
        self.close() 