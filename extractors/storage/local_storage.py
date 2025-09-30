"""
Local filesystem storage manager implementation.
This module provides a storage manager for local filesystem.
"""

import os
import json
import logging
import time
import glob
from typing import Dict, List, Any, Optional, Tuple, Union

from extractors.base.base_storage_manager import BaseStorageManager


class LocalStorageManager(BaseStorageManager):
    """Local filesystem storage manager implementation.
    
    Attributes:
        config (Dict): Local storage configuration
        base_path (str): Base directory for storage
        logger (logging.Logger): Logger for this storage manager
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the local storage manager.
        
        Args:
            config: Dictionary containing local storage configuration
                Required keys: base_path
                Optional keys: create_dirs, timestamp_dirs
        """
        super().__init__(config)
        self.base_path = config['base_path']
        self.create_dirs = config.get('create_dirs', True)
        self.timestamp_dirs = config.get('timestamp_dirs', False)
        
    def connect(self) -> bool:
        """Connect to local filesystem by validating and creating the base directory.
        
        Returns:
            bool: True if connection was successful, False otherwise
        """
        try:
            # Ensure base directory exists
            if not os.path.exists(self.base_path):
                if self.create_dirs:
                    os.makedirs(self.base_path, exist_ok=True)
                    self.logger.info(f"Created base directory: {self.base_path}")
                else:
                    self.logger.error(f"Base directory does not exist: {self.base_path}")
                    return False
            
            # Validate it's a directory and writable
            if not os.path.isdir(self.base_path):
                self.logger.error(f"Base path is not a directory: {self.base_path}")
                return False
                
            if not os.access(self.base_path, os.W_OK):
                self.logger.error(f"Base directory is not writable: {self.base_path}")
                return False
                
            self.logger.info(f"Successfully connected to local storage: {self.base_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Local storage connection error: {str(e)}")
            return False
    
    def validate_storage(self) -> bool:
        """Validate access to the local filesystem.
        
        Returns:
            bool: True if storage is accessible, False otherwise
        """
        return self.connect()
    
    def _get_full_path(self, path: str) -> str:
        """Get the full filesystem path by combining base path and file path.
        
        Args:
            path: Relative file path
            
        Returns:
            str: Full filesystem path
        """
        # Handle timestamp directories if enabled
        if self.timestamp_dirs:
            date_dir = time.strftime("%Y-%m-%d")
            path = os.path.join(date_dir, path)
            
        return os.path.join(self.base_path, path)
    
    def _ensure_directory_exists(self, filepath: str) -> bool:
        """Ensure the directory for a file exists.
        
        Args:
            filepath: The full path to the file
            
        Returns:
            bool: True if directory exists or was created, False otherwise
        """
        directory = os.path.dirname(filepath)
        if not directory:
            return True
            
        try:
            if not os.path.exists(directory):
                if self.create_dirs:
                    os.makedirs(directory, exist_ok=True)
                    return True
                else:
                    self.logger.error(f"Directory does not exist and create_dirs is False: {directory}")
                    return False
            return True
        except Exception as e:
            self.logger.error(f"Error creating directory {directory}: {str(e)}")
            return False
    
    def store_data(self, 
                 data: Union[List[Dict[str, Any]], str, bytes], 
                 path: str,
                 metadata: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
        """Store data to local filesystem.
        
        Args:
            data: The data to store (can be list of dicts, string, or bytes)
            path: The relative path where the data should be stored
            metadata: Optional metadata to store with the data
            
        Returns:
            Tuple containing:
            - bool: True if storing was successful, False otherwise
            - str: Full path where the data was stored or error message
        """
        if not self.validate_storage():
            return False, "Storage validation failed"
            
        try:
            full_path = self._get_full_path(path)
            
            # Ensure directory exists
            if not self._ensure_directory_exists(full_path):
                return False, f"Failed to create directory for {full_path}"
                
            # Convert data to the appropriate format
            if isinstance(data, list):
                # Convert list of dicts to JSON string
                content = json.dumps(data, default=str, indent=2)
                write_mode = 'w'
                if not full_path.endswith('.json'):
                    full_path += '.json'
            elif isinstance(data, str):
                content = data
                write_mode = 'w'
            elif isinstance(data, bytes):
                content = data
                write_mode = 'wb'
            else:
                # Try to convert to JSON
                content = json.dumps(data, default=str, indent=2)
                write_mode = 'w'
                if not full_path.endswith('.json'):
                    full_path += '.json'
                
            # Write data to file
            if write_mode == 'w':
                with open(full_path, write_mode, encoding='utf-8') as f:
                    f.write(content)
            else:
                with open(full_path, write_mode) as f:
                    f.write(content)
                    
            # Store metadata if provided
            if metadata:
                metadata_path = f"{full_path}.metadata.json"
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    json.dump({
                        'timestamp': int(time.time()),
                        'record_count': len(data) if isinstance(data, list) else 0,
                        **metadata
                    }, f, indent=2)
                    
            self.logger.info(f"Successfully stored data to {full_path}")
            return True, full_path
            
        except Exception as e:
            error_msg = f"Error storing data to local filesystem: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def retrieve_data(self, 
                    path: str,
                    as_type: str = 'dict') -> Tuple[bool, Union[List[Dict[str, Any]], str, bytes, None]]:
        """Retrieve data from local filesystem.
        
        Args:
            path: The path from where to retrieve the data
            as_type: The return type ('dict', 'string', 'bytes')
            
        Returns:
            Tuple containing:
            - bool: True if retrieval was successful, False otherwise
            - Union[List[Dict], str, bytes, None]: The retrieved data or None if failed
        """
        try:
            full_path = self._get_full_path(path)
            
            # Check if file exists
            if not os.path.exists(full_path):
                self.logger.error(f"File not found: {full_path}")
                return False, None
                
            # Read data from file
            if as_type == 'bytes':
                with open(full_path, 'rb') as f:
                    content = f.read()
                return True, content
                
            if as_type == 'string' or (not path.endswith('.json') and as_type == 'dict'):
                with open(full_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                return True, content
                
            # Default is dict for JSON files
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                return True, json_data
            except json.JSONDecodeError:
                self.logger.warning(f"Retrieved file is not valid JSON: {full_path}")
                with open(full_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                return True, content
                
        except Exception as e:
            self.logger.error(f"Error retrieving data from local filesystem: {str(e)}")
            return False, None
    
    def list_files(self, path: str) -> List[str]:
        """List files in a specific directory.
        
        Args:
            path: The relative path to list files from
            
        Returns:
            List of file paths
        """
        try:
            full_path = self._get_full_path(path)
            
            # Ensure path is a directory
            if os.path.isfile(full_path):
                return [os.path.basename(full_path)]
                
            # Use glob pattern for directory contents
            if not full_path.endswith('/'):
                full_path += '/'
                
            glob_pattern = os.path.join(full_path, '*')
            files = glob.glob(glob_pattern)
            
            # Filter out metadata files and get relative paths
            relative_files = []
            for file_path in files:
                if file_path.endswith('.metadata.json'):
                    continue
                    
                # Convert to relative path
                if os.path.isabs(file_path):
                    rel_path = os.path.relpath(file_path, self.base_path)
                else:
                    rel_path = file_path
                    
                relative_files.append(rel_path)
                
            return relative_files
            
        except Exception as e:
            self.logger.error(f"Error listing files from local filesystem: {str(e)}")
            return []
    
    def close(self):
        """Close any open connections."""
        # No connections to close for local storage
        pass
