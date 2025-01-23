#!/usr/bin/env python3

import os
import json
import requests
from dotenv import load_dotenv
from pathlib import Path
from time import sleep

def load_query_from_file(file_path):
    """Load SQL query from file and return it as a string."""
    with open(file_path, 'r') as f:
        return f.read()

def replace_env_variables(query):
    """Replace environment variables placeholders in the query."""
    replacements = {
        'BOOTSTRAP_SERVERS': os.getenv('BOOTSTRAP_SERVERS'),
        'SASL_USERNAME': os.getenv('SASL_USERNAME'),
        'SASL_PASSWORD': os.getenv('SASL_PASSWORD'),
        'SCHEMA_REGISTRY_URL': os.getenv('SCHEMA_REGISTRY_URL'),
        'API_KEY': os.getenv('SASL_USERNAME'),
        'API_SECRET': os.getenv('SASL_PASSWORD')
    }
    
    query_text = query
    for key, value in replacements.items():
        if value is None:
            raise ValueError(f"Environment variable for {key} is not set")
        query_text = query_text.replace(key, value)
    return query_text

def execute_query(cluster_id, api_key, api_secret, query):
    """Execute a query using Confluent Cloud's REST API."""
    base_url = f"https://api.confluent.cloud/ksql/v2/clusters/{cluster_id}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # Create the query execution request
    data = {
        "sql_string": query,
        "properties": {
            "auto.offset.reset": "earliest",
            "ksql.streams.auto.offset.reset": "earliest"
        }
    }
    
    # Execute the query
    response = requests.post(
        f"{base_url}/ksql",
        headers=headers,
        auth=(api_key, api_secret),
        json=data
    )
    
    if response.status_code != 200:
        print(f"Error executing query: {response.text}")
        return False
    
    print(f"Query executed successfully: {response.json()}")
    return True

def main():
    # Load environment variables
    load_dotenv()
    
    # Get Confluent Cloud credentials
    cluster_id = os.getenv('CONFLUENT_CLUSTER_ID')
    api_key = os.getenv('CONFLUENT_API_KEY')
    api_secret = os.getenv('CONFLUENT_API_SECRET')
    
    if not all([cluster_id, api_key, api_secret]):
        raise ValueError("Missing required Confluent Cloud credentials")
    
    # Get the directory containing the queries
    current_dir = Path(__file__).parent
    
    # Define query files
    query_files = [
        current_dir / "flink_query_avg_duration.sql",
        current_dir / "flink_query_daily_engagement.sql"
    ]
    
    # Execute each query file
    for query_file in query_files:
        print(f"\nProcessing query file: {query_file.name}")
        try:
            # Load and prepare query
            query = load_query_from_file(query_file)
            query = replace_env_variables(query)
            
            # Execute query
            success = execute_query(cluster_id, api_key, api_secret, query)
            
            if success:
                print(f"Successfully processed {query_file.name}")
                # Wait a bit between queries
                sleep(5)
            else:
                print(f"Failed to process {query_file.name}")
                
        except Exception as e:
            print(f"Error processing {query_file.name}: {e}")

if __name__ == "__main__":
    main() 