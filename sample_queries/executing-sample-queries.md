# Executing Sample Queries

This guide explains how to execute the Flink SQL queries that analyze the Netflix viewing data stream and what each query does.

## Prerequisites

1. Apache Flink installed (via Homebrew on macOS):
   ```bash
   brew install apache-flink
   ```

2. Environment variables set in `.env` file:
   ```
   BOOTSTRAP_SERVERS=your-confluent-bootstrap-servers
   SASL_USERNAME=your-api-key
   SASL_PASSWORD=your-api-secret
   SCHEMA_REGISTRY_URL=your-schema-registry-url
   ```

## Running the Queries

Execute the queries using the provided script:

```bash
cd sample_queries
./execute_queries.sh
```

The script will:
1. Load your Confluent Cloud credentials from the `.env` file
2. Replace placeholders in the SQL files with your actual credentials
3. Start the Flink SQL Client with the necessary configuration
4. Execute both queries in sequence
5. Clean up temporary files

## Query Details

### Average Duration Analysis (`flink_query_avg_duration.sql`)

This query calculates movie viewing metrics:

```sql
CREATE TABLE movie_duration_metrics (
    title STRING,
    total_views BIGINT,
    avg_duration DOUBLE,
    total_duration BIGINT,
    genres STRING,
    release_date STRING,
    update_time TIMESTAMP(3)
)
```

Metrics calculated:
- Total views per movie
- Average viewing duration
- Total viewing duration
- Associated metadata (genres, release date)

The results are stored in the `netflix_movie_duration_metrics` Kafka topic.

### Daily Engagement Analysis (`flink_query_daily_engagement.sql`)

This query provides daily viewing behavior metrics:

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
)
```

Metrics calculated:
- Daily unique users
- Total views per day
- Total and average watch time
- Most popular title and genre
- Peak viewing hour

The results are stored in the `netflix_daily_engagement_metrics` Kafka topic.

## Monitoring Results

You can monitor the results using Confluent Cloud's Topic UI or by consuming from the output topics:
- `netflix_movie_duration_metrics`
- `netflix_daily_engagement_metrics`

## Troubleshooting

1. If queries fail to start:
   - Check that all environment variables are set correctly
   - Verify Confluent Cloud connectivity
   - Ensure the input topic exists and contains data

2. If queries run but produce no results:
   - Verify that data is being produced to the input topic
   - Check the Flink UI for any warnings or errors
   - Ensure the AVRO schema matches the data structure

3. Common issues:
   - Schema compatibility errors: Ensure the AVRO schema matches the data
   - Authentication failures: Check your Confluent credentials
   - Missing dependencies: Verify Flink SQL connectors are available 