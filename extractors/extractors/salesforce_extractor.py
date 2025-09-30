"""
Salesforce data extractor implementation.
This module orchestrates the extraction of data from Salesforce.
"""

import logging
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union

from extractors.base.api_connector import BaseAPIConnector
from extractors.base.base_storage_manager import BaseStorageManager
from extractors.extractors.base_extractor import BaseExtractor
from extractors.connectors.salesforce_connector import SalesforceConnector


class SalesforceExtractor(BaseExtractor):
    """Salesforce data extractor implementation.
    
    Attributes:
        connector (SalesforceConnector): The Salesforce connector
        storage (BaseStorageManager): The storage manager
        config (Dict): Extraction configuration
        logger (logging.Logger): Logger for this extractor
    """
    
    def __init__(self, 
                connector: SalesforceConnector, 
                storage: BaseStorageManager,
                config: Dict[str, Any]):
        """Initialize the Salesforce extractor.
        
        Args:
            connector: Salesforce connector
            storage: Storage manager for extracted data
            config: Extraction configuration
        """
        super().__init__(connector, storage, config)
        
        # Set default configuration
        self.config.setdefault('batch_size', 2000)
        self.config.setdefault('schema_extract', True)
        self.config.setdefault('extract_path', 'salesforce')
        self.config.setdefault('important_fields', ['Id'])
    
    def extract(self, 
               object_names: List[str],
               extraction_type: str = 'full',
               since_date: Optional[str] = None) -> Dict[str, Any]:
        """Extract data from Salesforce and store it.
        
        Args:
            object_names: List of Salesforce object names to extract
            extraction_type: Type of extraction ('full', 'incremental')
            since_date: For incremental extraction, extract data since this date
            
        Returns:
            Dictionary with extraction results and metadata
        """
        start_time = time.time()
        results = {
            'success': True,
            'extraction_type': extraction_type,
            'start_time': datetime.now().isoformat(),
            'object_results': {},
            'errors': [],
        }
        
        # Validate connection before proceeding
        if not self.connector.validate_connection():
            results['success'] = False
            results['errors'].append("Failed to connect to Salesforce")
            return results
        
        # Process each object
        for object_name in object_names:
            self.logger.info(f"Extracting {object_name} data ({extraction_type})")
            
            try:
                # Extract schema if configured
                if self.config.get('schema_extract'):
                    self._extract_schema(object_name)
                
                # Extract data based on extraction type
                if extraction_type.lower() == 'incremental' and since_date:
                    success, metadata = self.extract_incremental(object_name, since_date)
                else:
                    success, metadata = self.extract_full(object_name)
                    
                results['object_results'][object_name] = metadata
                
                if not success:
                    results['success'] = False
                    results['errors'].append(f"Failed to extract {object_name}")
                    
            except Exception as e:
                error_msg = f"Error extracting {object_name}: {str(e)}"
                self.logger.error(error_msg)
                results['success'] = False
                results['errors'].append(error_msg)
                results['object_results'][object_name] = {'success': False, 'error': str(e)}
        
        # Add summary information
        execution_time = time.time() - start_time
        results['end_time'] = datetime.now().isoformat()
        results['execution_time_seconds'] = round(execution_time, 2)
        
        # Store extraction summary
        summary_path = f"{self.config['extract_path']}/extraction_summary_{int(time.time())}.json"
        self.storage.store_data(results, summary_path)
        
        return results
    
    def extract_incremental(self, 
                          object_name: str,
                          since_date: str) -> Tuple[bool, Dict[str, Any]]:
        """Extract Salesforce data incrementally from the specified date.
        
        Args:
            object_name: The Salesforce object to extract
            since_date: Extract data modified since this date (ISO format)
            
        Returns:
            Tuple containing:
            - bool: True if extraction was successful, False otherwise
            - Dict: Metadata about the extraction
        """
        metadata = {
            'object_name': object_name,
            'extraction_type': 'incremental',
            'since_date': since_date,
            'start_time': datetime.now().isoformat(),
            'record_count': 0,
            'success': False,
        }
        
        try:
            # Build query parameters for incremental extraction
            query_params = {
                'fields': self.config.get('fields', {}).get(object_name, ['Id', 'Name', 'CreatedDate', 'LastModifiedDate']),
                'where': f"LastModifiedDate >= {since_date}",
                'limit': self.config.get('batch_size', 2000),
                'order_by': 'LastModifiedDate ASC'
            }
            
            # Fetch data
            records = self.connector.fetch_data(object_name, query_params)
            
            if not records:
                metadata['record_count'] = 0
                metadata['success'] = True
                metadata['message'] = f"No updated records found for {object_name} since {since_date}"
                return True, metadata
            
            # Validate data quality
            quality_results = self.validate_data_quality(records)
            metadata['data_quality'] = quality_results
            
            # Store the extracted data
            storage_path = f"{self.config['extract_path']}/{object_name}/incremental_{int(time.time())}"
            success, path = self.storage.store_data(records, storage_path, metadata={
                'extraction_type': 'incremental',
                'object_name': object_name,
                'since_date': since_date
            })
            
            metadata['record_count'] = len(records)
            metadata['storage_path'] = path
            metadata['success'] = success
            metadata['end_time'] = datetime.now().isoformat()
            
            self.logger.info(f"Extracted {len(records)} records from {object_name} (incremental)")
            return success, metadata
            
        except Exception as e:
            error_msg = f"Error during incremental extraction of {object_name}: {str(e)}"
            self.logger.error(error_msg)
            metadata['error'] = str(e)
            metadata['end_time'] = datetime.now().isoformat()
            return False, metadata
    
    def extract_full(self, 
                   object_name: str) -> Tuple[bool, Dict[str, Any]]:
        """Extract all data from the Salesforce object.
        
        Args:
            object_name: The Salesforce object to extract
            
        Returns:
            Tuple containing:
            - bool: True if extraction was successful, False otherwise
            - Dict: Metadata about the extraction
        """
        metadata = {
            'object_name': object_name,
            'extraction_type': 'full',
            'start_time': datetime.now().isoformat(),
            'record_count': 0,
            'success': False,
        }
        
        try:
            # Build query parameters for full extraction
            query_params = {
                'fields': self.config.get('fields', {}).get(object_name, ['Id', 'Name', 'CreatedDate', 'LastModifiedDate']),
                'limit': self.config.get('batch_size', 2000),
                'order_by': 'Id ASC'
            }
            
            # Fetch data
            records = self.connector.fetch_data(object_name, query_params)
            
            if not records:
                metadata['record_count'] = 0
                metadata['success'] = True
                metadata['message'] = f"No records found for {object_name}"
                return True, metadata
            
            # Validate data quality
            quality_results = self.validate_data_quality(records)
            metadata['data_quality'] = quality_results
            
            # Store the extracted data
            storage_path = f"{self.config['extract_path']}/{object_name}/full_{int(time.time())}"
            success, path = self.storage.store_data(records, storage_path, metadata={
                'extraction_type': 'full',
                'object_name': object_name
            })
            
            metadata['record_count'] = len(records)
            metadata['storage_path'] = path
            metadata['success'] = success
            metadata['end_time'] = datetime.now().isoformat()
            
            self.logger.info(f"Extracted {len(records)} records from {object_name} (full)")
            return success, metadata
            
        except Exception as e:
            error_msg = f"Error during full extraction of {object_name}: {str(e)}"
            self.logger.error(error_msg)
            metadata['error'] = str(e)
            metadata['end_time'] = datetime.now().isoformat()
            return False, metadata
    
    def _extract_schema(self, object_name: str) -> bool:
        """Extract and store the schema for a Salesforce object.
        
        Args:
            object_name: The Salesforce object to extract schema for
            
        Returns:
            bool: True if schema extraction was successful, False otherwise
        """
        try:
            # Fetch schema
            schema = self.connector.fetch_schema(object_name)
            
            if not schema:
                self.logger.warning(f"Failed to fetch schema for {object_name}")
                return False
            
            # Store schema
            schema_path = f"{self.config['extract_path']}/schemas/{object_name}_schema_{int(time.time())}"
            success, _ = self.storage.store_data(schema, schema_path)
            
            if success:
                self.logger.info(f"Successfully extracted schema for {object_name}")
            else:
                self.logger.warning(f"Successfully fetched but failed to store schema for {object_name}")
                
            return success
            
        except Exception as e:
            self.logger.error(f"Error extracting schema for {object_name}: {str(e)}")
            return False