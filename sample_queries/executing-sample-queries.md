# Executing Sample Queries

This guide explains how to execute the SQL queries that analyze the Netflix viewing data stream using Confluent Cloud's SQL interface.

## Prerequisites

1. Python 3.8+ with required packages:
   ```bash
   pip install requests python-dotenv
   ```

2. Environment variables set in `.env` file:
   ```
   # Confluent Cloud Configuration
   BOOTSTRAP_SERVERS='your-bootstrap-servers'
   SASL_USERNAME='your-api-key'
   SASL_PASSWORD='your-api-secret'
   SCHEMA_REGISTRY_URL='your-schema-registry-url'
   
   # Confluent Cloud API Access
   CONFLUENT_CLUSTER_ID='your-cluster-id'
   CONFLUENT_API_KEY='your-api-key'
   CONFLUENT_API_SECRET='your-api-secret'
   ```

   You can find these values in your Confluent Cloud console:
   - Cluster ID: Under "Cluster Overview" > "Cluster Settings"
   - API Keys: Under "Cluster Overview" > "API Keys"

## Running the Queries

Execute the queries using the provided Python script:

```bash
cd sample_queries
python execute_queries.py
```

The script will:
1. Load your Confluent Cloud credentials from the `.env` file
2. Connect to your Confluent Cloud cluster using the REST API
3. Execute both queries in sequence
4. Report the status of each query execution

## Query Details

### Average Duration Analysis (`flink_query_avg_duration.sql`)

This query creates a materialized view of movie viewing metrics:

```sql
CREATE TABLE movie_duration_metrics (
    title STRING,
    total_views BIGINT,
    avg_duration DOUBLE,
    total_duration BIGINT,
    genres STRING,
    release_date STRING,
    update_time TIMESTAMP(3)
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'netflix_movie_duration_metrics',
    'key.format' = 'json',
    'value.format' = 'json'
);
```

Metrics calculated:
- Total views per movie
- Average viewing duration
- Total viewing duration
- Associated metadata (genres, release date)

The results are continuously updated in the `netflix_movie_duration_metrics` topic.

### Daily Engagement Analysis (`flink_query_daily_engagement.sql`)

This query creates a materialized view of daily viewing behavior:

```sql
CREATE TABLE daily_engagement_metrics (
    view_date DATE,
    total_unique_users BIGINT,
    total_views BIGINT,
    total_watch_time BIGINT,
    avg_watch_time DOUBLE,
    most_watched_title STRING,
    most_watched_genre STRING,
    peak_hour INT,
    update_time TIMESTAMP(3)
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'netflix_daily_engagement_metrics',
    'key.format' = 'json',
    'value.format' = 'json'
);
```

Metrics calculated:
- Daily unique users
- Total views per day
- Total and average watch time
- Most popular title and genre
- Peak viewing hour

The results are continuously updated in the `netflix_daily_engagement_metrics` topic.

## Monitoring Results

You can monitor the results in several ways:

1. Confluent Cloud Console:
   - Navigate to "Topics" and select the output topics
   - Use the "Messages" tab to view the latest metrics
   - Use the "Schema" tab to verify the data structure

2. ksqlDB Editor:
   ```sql
   -- View latest movie metrics
   SELECT * FROM netflix_movie_duration_metrics EMIT CHANGES;
   
   -- View latest daily metrics
   SELECT * FROM netflix_daily_engagement_metrics EMIT CHANGES;
   ```

3. Confluent Cloud CLI:
   ```bash
   confluent kafka topic consume netflix_movie_duration_metrics
   confluent kafka topic consume netflix_daily_engagement_metrics
   ```

## Troubleshooting

1. If queries fail to start:
   - Verify your Confluent Cloud credentials
   - Check that the cluster ID is correct
   - Ensure you have the necessary permissions

2. If queries run but produce no results:
   - Verify that data is flowing into the input topic
   - Check the query execution status in Confluent Cloud
   - Review the query logs for any errors

3. Common issues:
   - Authentication errors: Double-check your API credentials
   - Schema compatibility: Ensure the AVRO schema matches your data
   - Rate limiting: Check your API usage quotas
   - Network connectivity: Verify your network can reach Confluent Cloud 