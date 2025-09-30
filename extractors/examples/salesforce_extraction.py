#!/usr/bin/env python

"""
Example script for Salesforce data extraction.
This script demonstrates how to use the extraction framework to extract Salesforce data.
"""

import os
import logging
import json
import argparse
from datetime import datetime, timedelta

from extractors.connectors.salesforce_connector import SalesforceConnector
from extractors.storage.s3_storage import S3StorageManager
from extractors.storage.local_storage import LocalStorageManager
from extractors.extractors.salesforce_extractor import SalesforceExtractor


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("salesforce_extraction")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Salesforce data extraction tool")
    
    # Extraction type
    parser.add_argument(
        "--type", 
        choices=["full", "incremental"], 
        default="full",
        help="Type of extraction (full or incremental)"
    )
    
    # Objects to extract
    parser.add_argument(
        "--objects", 
        nargs="+", 
        default=["Account", "Contact"],
        help="Salesforce objects to extract"
    )
    
    # Connection method
    parser.add_argument(
        "--connection-file", 
        help="Path to JSON file with connection details"
    )
    
    # Storage options
    parser.add_argument(
        "--storage", 
        choices=["s3", "local"], 
        default="local",
        help="Storage type"
    )
    
    # Output directory for local storage
    parser.add_argument(
        "--output-dir", 
        default="./extracted_data",
        help="Output directory for local storage"
    )
    
    # S3 bucket for S3 storage
    parser.add_argument(
        "--s3-bucket",
        help="S3 bucket name for S3 storage"
    )
    
    # Days to look back for incremental extraction
    parser.add_argument(
        "--days", 
        type=int, 
        default=7,
        help="Days to look back for incremental extraction"
    )
    
    return parser.parse_args()


def load_connection_config(connection_file):
    """Load connection configuration from a JSON file."""
    if not os.path.exists(connection_file):
        raise FileNotFoundError(f"Connection file not found: {connection_file}")
        
    with open(connection_file, 'r') as f:
        return json.load(f)


def get_connector(connection_config):
    """Create and authenticate a Salesforce connector."""
    connector = SalesforceConnector(
        credentials=connection_config,
        rate_limit_config={
            "max_requests_per_minute": 100
        }
    )
    
    if not connector.authenticate():
        raise Exception("Failed to authenticate with Salesforce")
        
    return connector


def get_storage(args):
    """Create an appropriate storage manager based on arguments."""
    if args.storage == "s3":
        if not args.s3_bucket:
            raise ValueError("S3 bucket must be specified for S3 storage")
            
        # Try to get AWS credentials from environment
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        
        if not aws_access_key or not aws_secret_key:
            raise ValueError("AWS credentials not found in environment variables")
            
        return S3StorageManager({
            "aws_access_key": aws_access_key,
            "aws_secret_key": aws_secret_key,
            "bucket_name": args.s3_bucket,
            "region_name": os.environ.get("AWS_REGION", "us-east-1"),
            "base_path": "salesforce_data"
        })
    else:
        return LocalStorageManager({
            "base_path": args.output_dir,
            "create_dirs": True,
            "timestamp_dirs": True
        })


def main():
    """Main execution function."""
    args = parse_args()
    
    try:
        # Load connection configuration
        if args.connection_file:
            connection_config = load_connection_config(args.connection_file)
        else:
            # If no file provided, try environment variables
            connection_config = {
                "client_id": os.environ.get("SALESFORCE_CLIENT_ID"),
                "client_secret": os.environ.get("SALESFORCE_CLIENT_SECRET"),
                "username": os.environ.get("SALESFORCE_USERNAME"),
                "password": os.environ.get("SALESFORCE_PASSWORD"),
                "security_token": os.environ.get("SALESFORCE_SECURITY_TOKEN", ""),
                "sandbox": os.environ.get("SALESFORCE_SANDBOX", "false").lower() == "true"
            }
            
            # Validate required credentials
            missing = [k for k, v in connection_config.items() 
                     if k in ["client_id", "client_secret", "username", "password"] and not v]
            
            if missing:
                raise ValueError(f"Missing required Salesforce credentials: {', '.join(missing)}")
        
        # Create connector
        connector = get_connector(connection_config)
        
        # Create storage
        storage = get_storage(args)
        
        # Create extractor
        extractor = SalesforceExtractor(
            connector=connector,
            storage=storage,
            config={
                "batch_size": 2000,
                "schema_extract": True,
                "extract_path": "salesforce",
                "fields": {
                    # Define specific fields for each object if needed
                    "Account": ["Id", "Name", "Type", "Industry", "BillingAddress", "CreatedDate", "LastModifiedDate"],
                    "Contact": ["Id", "FirstName", "LastName", "Email", "Phone", "AccountId", "CreatedDate", "LastModifiedDate"],
                    # Default fields will be used for other objects
                },
                "important_fields": ["Id"]
            }
        )
        
        # Determine extraction parameters
        if args.type == "incremental":
            # Calculate start date for incremental extraction
            since_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%dT00:00:00Z")
            logger.info(f"Performing incremental extraction since {since_date}")
            
            # Execute extraction
            results = extractor.extract(
                object_names=args.objects,
                extraction_type="incremental",
                since_date=since_date
            )
        else:
            logger.info(f"Performing full extraction")
            
            # Execute extraction
            results = extractor.extract(
                object_names=args.objects,
                extraction_type="full"
            )
        
        # Log results
        success_count = sum(1 for obj, data in results["object_results"].items() if data.get("success", False))
        total_count = len(results["object_results"])
        total_records = sum(data.get("record_count", 0) for data in results["object_results"].values())
        
        logger.info(f"Extraction completed: {success_count}/{total_count} objects successful")
        logger.info(f"Total records extracted: {total_records}")
        
        if not results["success"]:
            logger.warning("Some objects failed to extract:")
            for error in results["errors"]:
                logger.warning(f"  - {error}")
                
        logger.info(f"Execution time: {results.get('execution_time_seconds', 0)} seconds")
        
    except Exception as e:
        logger.error(f"Error during extraction: {str(e)}")
        return 1
        
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code) 