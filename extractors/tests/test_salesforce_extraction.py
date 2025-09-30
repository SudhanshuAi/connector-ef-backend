#!/usr/bin/env python

"""
Test script for Salesforce data extraction.
This script validates the extraction framework with mock data.
"""

import unittest
import logging
from unittest.mock import MagicMock, patch
import json
import os
from datetime import datetime

from extractors.connectors.salesforce_connector import SalesforceConnector
from extractors.storage.local_storage import LocalStorageManager
from extractors.extractors.salesforce_extractor import SalesforceExtractor


class TestSalesforceExtraction(unittest.TestCase):
    """Test cases for Salesforce extraction framework."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Mock Salesforce credentials
        self.mock_credentials = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "username": "test@example.com",
            "password": "test_password",
            "security_token": "test_token",
            "sandbox": False
        }
        
        # Create temporary directory for test data
        self.test_dir = "/tmp/salesforce_test_data"
        os.makedirs(self.test_dir, exist_ok=True)
        
        # Mock data
        self.mock_accounts = [
            {"Id": "001A", "Name": "Test Account 1", "Industry": "Technology", "CreatedDate": "2023-01-01T00:00:00Z", "LastModifiedDate": "2023-01-02T00:00:00Z"},
            {"Id": "001B", "Name": "Test Account 2", "Industry": "Healthcare", "CreatedDate": "2023-01-03T00:00:00Z", "LastModifiedDate": "2023-01-04T00:00:00Z"},
        ]
        
        self.mock_contacts = [
            {"Id": "003A", "FirstName": "John", "LastName": "Doe", "Email": "john@example.com", "AccountId": "001A", "CreatedDate": "2023-01-01T00:00:00Z", "LastModifiedDate": "2023-01-02T00:00:00Z"},
            {"Id": "003B", "FirstName": "Jane", "LastName": "Smith", "Email": "jane@example.com", "AccountId": "001B", "CreatedDate": "2023-01-03T00:00:00Z", "LastModifiedDate": "2023-01-04T00:00:00Z"},
        ]
        
        self.mock_schema = {
            "name": "Account",
            "label": "Account",
            "fields": {
                "Id": {"type": "id", "label": "Account ID", "length": 18, "nillable": False},
                "Name": {"type": "string", "label": "Account Name", "length": 255, "nillable": False},
                "Industry": {"type": "picklist", "label": "Industry", "length": 40, "nillable": True},
                "CreatedDate": {"type": "datetime", "label": "Created Date", "nillable": False},
                "LastModifiedDate": {"type": "datetime", "label": "Last Modified Date", "nillable": False}
            },
            "timestamp": datetime.now().isoformat()
        }
    
    def tearDown(self):
        """Clean up after tests."""
        # Clean up test directory - uncomment if you want to inspect output
        # import shutil
        # shutil.rmtree(self.test_dir, ignore_errors=True)
        pass
    
    @patch('extractors.connectors.salesforce_connector.SalesforceConnector')
    def test_full_extraction(self, mock_connector_class):
        """Test full extraction of Salesforce data."""
        # Configure mock connector
        mock_connector = MagicMock()
        mock_connector_class.return_value = mock_connector
        mock_connector.authenticate.return_value = True
        mock_connector.validate_connection.return_value = True
        mock_connector.fetch_schema.return_value = self.mock_schema
        
        # Mock the fetch_data method to return different data based on object name
        def mock_fetch_data(object_name, query_params=None):
            if object_name == "Account":
                return self.mock_accounts
            elif object_name == "Contact":
                return self.mock_contacts
            return []
            
        mock_connector.fetch_data.side_effect = mock_fetch_data
        
        # Create real storage
        storage = LocalStorageManager({
            "base_path": self.test_dir,
            "create_dirs": True,
            "timestamp_dirs": False
        })
        
        # Create extractor
        extractor = SalesforceExtractor(
            connector=mock_connector,
            storage=storage,
            config={
                "batch_size": 2000,
                "schema_extract": True,
                "extract_path": "salesforce",
                "important_fields": ["Id", "Name"]
            }
        )
        
        # Run extraction
        results = extractor.extract(
            object_names=["Account", "Contact"],
            extraction_type="full"
        )
        
        # Verify results
        self.assertTrue(results["success"])
        self.assertEqual(len(results["object_results"]), 2)
        
        # Verify Account extraction
        account_result = results["object_results"]["Account"]
        self.assertTrue(account_result["success"])
        self.assertEqual(account_result["record_count"], 2)
        
        # Verify Contact extraction
        contact_result = results["object_results"]["Contact"]
        self.assertTrue(contact_result["success"])
        self.assertEqual(contact_result["record_count"], 2)
        
        # Verify method calls
        mock_connector.validate_connection.assert_called_once()
        self.assertEqual(mock_connector.fetch_schema.call_count, 2)
        self.assertEqual(mock_connector.fetch_data.call_count, 2)
    
    @patch('extractors.connectors.salesforce_connector.SalesforceConnector')
    def test_incremental_extraction(self, mock_connector_class):
        """Test incremental extraction of Salesforce data."""
        # Configure mock connector
        mock_connector = MagicMock()
        mock_connector_class.return_value = mock_connector
        mock_connector.authenticate.return_value = True
        mock_connector.validate_connection.return_value = True
        mock_connector.fetch_schema.return_value = self.mock_schema
        
        # Return only one record for incremental extraction
        def mock_fetch_data(object_name, query_params=None):
            if object_name == "Account":
                # Only return the second account (as if it was recently modified)
                return [self.mock_accounts[1]]
            elif object_name == "Contact":
                # Only return the second contact (as if it was recently modified)
                return [self.mock_contacts[1]]
            return []
            
        mock_connector.fetch_data.side_effect = mock_fetch_data
        
        # Create real storage
        storage = LocalStorageManager({
            "base_path": self.test_dir,
            "create_dirs": True,
            "timestamp_dirs": False
        })
        
        # Create extractor
        extractor = SalesforceExtractor(
            connector=mock_connector,
            storage=storage,
            config={
                "batch_size": 2000,
                "schema_extract": True,
                "extract_path": "salesforce",
                "important_fields": ["Id", "Name"]
            }
        )
        
        # Run incremental extraction
        since_date = "2023-01-03T00:00:00Z"
        results = extractor.extract(
            object_names=["Account", "Contact"],
            extraction_type="incremental",
            since_date=since_date
        )
        
        # Verify results
        self.assertTrue(results["success"])
        self.assertEqual(len(results["object_results"]), 2)
        
        # Verify Account extraction
        account_result = results["object_results"]["Account"]
        self.assertTrue(account_result["success"])
        self.assertEqual(account_result["record_count"], 1)
        
        # Verify Contact extraction
        contact_result = results["object_results"]["Contact"]
        self.assertTrue(contact_result["success"])
        self.assertEqual(contact_result["record_count"], 1)
        
        # Verify query parameters
        for call in mock_connector.fetch_data.call_args_list:
            args, kwargs = call
            query_params = args[1]
            self.assertIn("where", query_params)
            self.assertTrue(query_params["where"].startswith("LastModifiedDate >="))
    
    @patch('extractors.connectors.salesforce_connector.SalesforceConnector')
    def test_error_handling(self, mock_connector_class):
        """Test error handling during extraction."""
        # Configure mock connector
        mock_connector = MagicMock()
        mock_connector_class.return_value = mock_connector
        mock_connector.authenticate.return_value = True
        mock_connector.validate_connection.return_value = True
        
        # Mock fetch_schema to fail for one object
        def mock_fetch_schema(object_name):
            if object_name == "Account":
                return self.mock_schema
            elif object_name == "Contact":
                raise Exception("Schema error")
            return {}
            
        mock_connector.fetch_schema.side_effect = mock_fetch_schema
        
        # Mock fetch_data to succeed for Account but fail for Contact
        def mock_fetch_data(object_name, query_params=None):
            if object_name == "Account":
                return self.mock_accounts
            elif object_name == "Contact":
                raise Exception("Data error")
            return []
            
        mock_connector.fetch_data.side_effect = mock_fetch_data
        
        # Create real storage
        storage = LocalStorageManager({
            "base_path": self.test_dir,
            "create_dirs": True,
            "timestamp_dirs": False
        })
        
        # Create extractor
        extractor = SalesforceExtractor(
            connector=mock_connector,
            storage=storage,
            config={
                "batch_size": 2000,
                "schema_extract": True,
                "extract_path": "salesforce",
                "important_fields": ["Id", "Name"]
            }
        )
        
        # Run extraction
        results = extractor.extract(
            object_names=["Account", "Contact"],
            extraction_type="full"
        )
        
        # Verify results
        self.assertFalse(results["success"])
        self.assertEqual(len(results["errors"]), 1)
        self.assertEqual(len(results["object_results"]), 2)
        
        # Verify Account extraction succeeded
        account_result = results["object_results"]["Account"]
        self.assertTrue(account_result["success"])
        
        # Verify Contact extraction failed
        contact_result = results["object_results"]["Contact"]
        self.assertFalse(contact_result["success"])
        self.assertIn("error", contact_result)


if __name__ == "__main__":
    unittest.main() 