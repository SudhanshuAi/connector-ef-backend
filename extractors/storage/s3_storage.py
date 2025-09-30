"""
S3 storage manager implementation.
This module provides a storage manager for AWS S3.
"""

import json
import logging
import io
import time
from typing import Dict, List, Any, Optional, Tuple, Union
import boto3
from botocore.exceptions import ClientError

from extractors.base.base_storage_manager import BaseStorageManager


class S3StorageManager(BaseStorageManager):
    """AWS S3 storage manager implementation.
    
    Attributes:
        config (Dict): S3 configuration
        s3_client: Boto3 S3 client
        bucket_name (str): S3 bucket name
        base_path (str): Base path prefix for S3 objects
        logger (logging.Logger): Logger for this storage manager
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the S3 storage manager.
        
        Args:
            config: Dictionary containing S3 configuration
                Required keys: aws_access_key, aws_secret_key, bucket_name
                Optional keys: region_name, base_path, endpoint_url
        """
        super().__init__(config)
        self.bucket_name = config['bucket_name']
        self.base_path = config.get('base_path', '')
        self.s3_client = None
        
    def connect(self) -> bool:
        """Connect to AWS S3 using the provided credentials.
        
        Returns:
            bool: True if connection was successful, False otherwise
        """
        try:
            # Initialize S3 client
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.config['aws_access_key'],
                aws_secret_access_key=self.config['aws_secret_key'],
                region_name=self.config.get('region_name', 'us-east-1'),
                endpoint_url=self.config.get('endpoint_url')
            )
            
            # Test connection by checking if bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            self.logger.info(f"Successfully connected to S3 bucket: {self.bucket_name}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.logger.error(f"Bucket {self.bucket_name} does not exist")
            elif error_code == '403':
                self.logger.error(f"Access denied to bucket {self.bucket_name}")
            else:
                self.logger.error(f"S3 connection error: {str(e)}")
            return False
            
        except Exception as e:
            self.logger.error(f"S3 connection error: {str(e)}")
            return False
    
    def validate_storage(self) -> bool:
        """Validate the connection to S3 and check bucket permissions.
        
        Returns:
            bool: True if storage is accessible, False otherwise
        """
        if not self.s3_client:
            return self.connect()
            
        try:
            # Check if we can list objects in the bucket (test permissions)
            self.s3_client.list_objects_v2(Bucket=self.bucket_name, MaxKeys=1)
            return True
            
        except Exception as e:
            self.logger.error(f"S3 storage validation failed: {str(e)}")
            return False
    
    def _get_full_path(self, path: str) -> str:
        """Get the full S3 path by combining base path and object path.
        
        Args:
            path: Object path
            
        Returns:
            str: Full S3 path
        """
        if not self.base_path:
            return path
        
        # Ensure base_path doesn't end with slash and path doesn't start with slash
        base = self.base_path.rstrip('/')
        relative = path.lstrip('/')
        
        return f"{base}/{relative}"
    
    def store_data(self, 
                 data: Union[List[Dict[str, Any]], str, bytes], 
                 path: str,
                 metadata: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
        """Store data to S3.
        
        Args:
            data: The data to store (can be list of dicts, string, or bytes)
            path: The path where the data should be stored
            metadata: Optional metadata to store with the data
            
        Returns:
            Tuple containing:
            - bool: True if storing was successful, False otherwise
            - str: S3 path where the data was stored or error message
        """
        if not self.validate_storage():
            return False, "Storage validation failed"
            
        try:
            full_path = self._get_full_path(path)
            
            # Convert data to the appropriate format
            if isinstance(data, list):
                # Convert list of dicts to JSON string
                content = json.dumps(data, default=str)
                content_type = 'application/json'
            elif isinstance(data, str):
                # Use string as is
                content = data
                content_type = 'text/plain'
            elif isinstance(data, bytes):
                # Use bytes as is
                content = data
                content_type = 'application/octet-stream'
            else:
                # Try to convert to JSON
                content = json.dumps(data, default=str)
                content_type = 'application/json'
                
            # Prepare S3 metadata
            s3_metadata = metadata or {}
            s3_extra_args = {
                'ContentType': content_type,
                'Metadata': {
                    'timestamp': str(int(time.time())),
                    'record_count': str(len(data) if isinstance(data, list) else 0),
                    **{k: str(v) for k, v in s3_metadata.items()}
                }
            }
            
            # Upload to S3
            if isinstance(content, str):
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=full_path,
                    Body=content.encode('utf-8'),
                    **s3_extra_args
                )
            else:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=full_path,
                    Body=content,
                    **s3_extra_args
                )
                
            s3_uri = f"s3://{self.bucket_name}/{full_path}"
            self.logger.info(f"Successfully stored data to {s3_uri}")
            return True, s3_uri
            
        except Exception as e:
            error_msg = f"Error storing data to S3: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def retrieve_data(self, 
                    path: str,
                    as_type: str = 'dict') -> Tuple[bool, Union[List[Dict[str, Any]], str, bytes, None]]:
        """Retrieve data from S3.
        
        Args:
            path: The S3 path from where to retrieve the data
            as_type: The return type ('dict', 'string', 'bytes')
            
        Returns:
            Tuple containing:
            - bool: True if retrieval was successful, False otherwise
            - Union[List[Dict], str, bytes, None]: The retrieved data or None if failed
        """
        if not self.validate_storage():
            return False, None
            
        try:
            full_path = self._get_full_path(path)
            
            # Get object from S3
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=full_path
            )
            
            # Read data from response
            content = response['Body'].read()
            
            # Convert to requested format
            if as_type == 'bytes':
                return True, content
            
            if as_type == 'string':
                return True, content.decode('utf-8')
            
            # Default is dict
            try:
                json_data = json.loads(content.decode('utf-8'))
                return True, json_data
            except json.JSONDecodeError:
                self.logger.warning(f"Retrieved data is not valid JSON: {path}")
                return True, content.decode('utf-8')
                
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                self.logger.error(f"Object not found: {path}")
            else:
                self.logger.error(f"Error retrieving data from S3: {str(e)}")
            return False, None
            
        except Exception as e:
            self.logger.error(f"Error retrieving data from S3: {str(e)}")
            return False, None
    
    def list_files(self, path: str) -> List[str]:
        """List files in a specific S3 path.
        
        Args:
            path: The path to list files from
            
        Returns:
            List of file paths
        """
        if not self.validate_storage():
            return []
            
        try:
            full_path = self._get_full_path(path)
            
            # Ensure path ends with a slash for proper prefix filtering
            if full_path and not full_path.endswith('/'):
                full_path += '/'
                
            # List objects in S3
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=full_path
            )
            
            # Extract file paths
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Remove base path prefix to get relative path
                    relative_path = obj['Key']
                    if self.base_path:
                        base_prefix = f"{self.base_path.rstrip('/')}/"
                        if relative_path.startswith(base_prefix):
                            relative_path = relative_path[len(base_prefix):]
                    files.append(relative_path)
                    
            return files
            
        except Exception as e:
            self.logger.error(f"Error listing files from S3: {str(e)}")
            return []
    
    def close(self):
        """Close any open connections."""
        self.s3_client = None
