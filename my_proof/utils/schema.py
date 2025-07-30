import json
import os
import logging
import csv
from typing import Dict, Any, Tuple

import jsonschema

def validate_schema(input_data: Dict[str, Any]) -> Tuple[str, bool]:
    """
    Validate input data against schemas using jsonschema.
    Supports both JSON (Google profile) and CSV (Netflix data) files.
    
    Args:
        input_data: The data to validate (JSON dict or CSV metadata)
        
    Returns:
        tuple[str, bool]: A tuple containing (schema_type, is_valid)
        where schema_type is the schema name and is_valid indicates if validation passed
    """
    try:
        # Check if this is Netflix CSV data
        if 'file_type' in input_data and input_data.get('data_format') == 'csv':
            schema_type = 'netflix-csv.json'
        else:
            # Default to Google profile schema
            schema_type = 'google-profile.json'
        
        # Load the schema
        schema_path = os.path.join(os.path.dirname(__file__), '..', 'schemas', schema_type)
        with open(schema_path, 'r') as f:
            schema = json.load(f)
            
        # Validate against schema
        jsonschema.validate(instance=input_data, schema=schema)
        return schema_type, True
        
    except jsonschema.exceptions.ValidationError as e:
        logging.error(f"Schema validation error: {str(e)}")
        return schema_type, False
    except Exception as e:
        logging.error(f"Schema validation failed: {str(e)}")
        return schema_type, False

def analyze_csv_file(file_path: str) -> Dict[str, Any]:
    """
    Analyze a CSV file and return metadata for validation.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        Dict containing metadata about the CSV file
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Read first few lines to determine file type
            lines = f.readlines()
            
        if not lines:
            return {"error": "Empty file"}
            
        # Parse CSV header
        reader = csv.reader(lines)
        header = next(reader)
        
        # Determine file type based on header
        file_type = "netflix-viewing-activity"  # Default
        if any("billing" in col.lower() or "payment" in col.lower() for col in header):
            file_type = "netflix-billing-history"
            
        # Count records (excluding header)
        record_count = len(lines) - 1
        
        # Get file size
        file_size_bytes = os.path.getsize(file_path)
        
        return {
            "file_type": file_type,
            "data_format": "csv",
            "record_count": record_count,
            "file_size_bytes": file_size_bytes,
            "has_viewing_data": file_type == "netflix-viewing-activity",
            "has_billing_data": file_type == "netflix-billing-history"
        }
        
    except Exception as e:
        logging.error(f"CSV analysis failed: {str(e)}")
        return {"error": str(e)}
