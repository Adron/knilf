#!/usr/bin/env python3

import os
import json
import requests
from dotenv import load_dotenv
from pathlib import Path
from time import sleep

def execute_ksql_query(query):
    """Execute a query using Confluent Cloud's ksqlDB REST API."""
    endpoint = os.getenv('KSQLDB_ENDPOINT')
    api_key = os.getenv('KSQLDB_API_KEY')
    api_secret = os.getenv('KSQLDB_API_SECRET')
    
    headers = {
        "Content-Type": "application/vnd.ksql.v1+json",
        "Accept": "application/vnd.ksql.v1+json"
    }
    
    # Create the query execution request
    data = {
        "ksql": query,
        "streamsProperties": {
            "ksql.streams.auto.offset.reset": "earliest"
        }
    }
    
    # Execute the query
    try:
        # Print request details for debugging
        print(f"\nMaking request to: {endpoint}/ksql")
        print(f"Headers: {headers}")
        print(f"Query: {query}")
        
        response = requests.post(
            f"{endpoint}/ksql",
            headers=headers,
            auth=(api_key, api_secret),
            json=data,
            verify=True
        )
        
        print(f"Response Status: {response.status_code}")
        
        if response.status_code == 200:
            print("Query executed successfully:")
            print(json.dumps(response.json(), indent=2))
            return True
        else:
            print(f"Error executing query (Status {response.status_code}):")
            print(json.dumps(response.json(), indent=2))
            return False
    except Exception as e:
        print(f"Error making request: {e}")
        return False

def main():
    # Load environment variables
    load_dotenv()
    
    # Verify required environment variables
    required_vars = ['KSQLDB_ENDPOINT', 'KSQLDB_API_KEY', 'KSQLDB_API_SECRET']
    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"Missing required environment variable: {var}")
    
    # Define the queries
    queries = [
        # First, drop existing streams and tables if they exist
        "TERMINATE ALL;",
        "DROP TABLE IF EXISTS DAILY_ENGAGEMENT_METRICS DELETE TOPIC;",
        "DROP TABLE IF EXISTS MOVIE_DURATION_METRICS DELETE TOPIC;",
        "DROP STREAM IF EXISTS NETFLIX_ACTIVITY DELETE TOPIC;",
        
        # Create the source stream with correct timestamp handling
        """
        CREATE STREAM IF NOT EXISTS netflix_activity (
            USER_ID STRING,
            TIMESTAMP STRING,
            ACTIVITY_TYPE STRING,
            TITLE_ID STRING,
            DURATION INT,
            DEVICE_TYPE STRING,
            LOCATION STRING,
            TITLE STRING,
            GENRES STRING,
            RELEASE_DATE STRING,
            MOVIE_ID STRING
        ) WITH (
            KAFKA_TOPIC='netflix_browsing_activity',
            PARTITIONS=6,
            REPLICAS=3,
            VALUE_FORMAT='AVRO',
            TIMESTAMP='TIMESTAMP',
            TIMESTAMP_FORMAT='yyyy-MM-dd HH:mm:ss'
        );
        """,
        
        # Create movie duration metrics table
        """
        CREATE TABLE IF NOT EXISTS movie_duration_metrics WITH (
            KAFKA_TOPIC='netflix_movie_duration_metrics',
            PARTITIONS=6,
            REPLICAS=3,
            VALUE_FORMAT='JSON'
        ) AS
        SELECT
            TITLE,
            COUNT(*) AS total_views,
            AVG(CAST(DURATION AS DOUBLE)) AS avg_duration,
            SUM(DURATION) AS total_duration,
            LATEST_BY_OFFSET(GENRES) AS genres,
            LATEST_BY_OFFSET(RELEASE_DATE) AS release_date,
            TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss') AS window_start
        FROM netflix_activity
        WINDOW TUMBLING (SIZE 1 DAY)
        WHERE DURATION IS NOT NULL
        GROUP BY TITLE
        EMIT CHANGES;
        """,
        
        # Create daily engagement metrics table
        """
        CREATE TABLE IF NOT EXISTS daily_engagement_metrics WITH (
            KAFKA_TOPIC='netflix_daily_engagement_metrics',
            PARTITIONS=6,
            REPLICAS=3,
            VALUE_FORMAT='JSON'
        ) AS
        SELECT
            TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd') AS view_date,
            COUNT_DISTINCT(USER_ID) AS total_unique_users,
            COUNT(*) AS total_views,
            SUM(DURATION) AS total_watch_time,
            AVG(CAST(DURATION AS DOUBLE)) AS avg_watch_time,
            LATEST_BY_OFFSET(TITLE) AS most_watched_title,
            LATEST_BY_OFFSET(GENRES) AS most_watched_genre,
            TIMESTAMPTOSTRING(WINDOWSTART, 'HH') AS window_hour
        FROM netflix_activity
        WINDOW TUMBLING (SIZE 1 DAY)
        WHERE DURATION IS NOT NULL
        GROUP BY WINDOWSTART
        EMIT CHANGES;
        """
    ]
    
    # Execute each query
    for i, query in enumerate(queries, 1):
        print(f"\nExecuting query {i} of {len(queries)}...")
        success = execute_ksql_query(query.strip())
        
        if success:
            print(f"Successfully executed query {i}")
            # Wait a bit between queries
            sleep(5)
        else:
            print(f"Failed to execute query {i}")
            # Continue with next query even if this one failed
            continue

if __name__ == "__main__":
    main() 