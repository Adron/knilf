#!/bin/bash

# Load environment variables
source ../.env

# Function to execute ksqlDB query
execute_ksql_query() {
    local query="$1"
    local escaped_query=$(echo "$query" | sed 's/"/\\"/g' | tr '\n' ' ')
    echo "Executing query: $query"
    curl -X POST "$KSQLDB_ENDPOINT/ksql" \
        -H "Content-Type: application/vnd.ksql.v1+json" \
        -H "Accept: application/vnd.ksql.v1+json" \
        -u "$KSQLDB_API_KEY:$KSQLDB_API_SECRET" \
        -d "{\"ksql\":\"$escaped_query\",\"streamsProperties\":{\"ksql.streams.auto.offset.reset\":\"earliest\"}}"
    echo
}

echo "Creating source stream..."
execute_ksql_query "CREATE STREAM IF NOT EXISTS netflix_activity (user_id STRING, timestamp STRING, activity_type STRING, title_id STRING, duration INTEGER, device_type STRING, location STRING, title STRING, genres STRING, release_date STRING, movie_id STRING) WITH (KAFKA_TOPIC='netflix_browsing_activity', VALUE_FORMAT='AVRO', TIMESTAMP='timestamp', TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ss');"

echo "Creating movie duration metrics stream..."
execute_ksql_query "CREATE TABLE IF NOT EXISTS movie_duration_metrics WITH (KAFKA_TOPIC='netflix_movie_duration_metrics', VALUE_FORMAT='JSON') AS SELECT title, COUNT(*) AS total_views, AVG(CAST(duration AS DOUBLE)) AS avg_duration, SUM(duration) AS total_duration, LATEST_BY_OFFSET(genres) AS genres, LATEST_BY_OFFSET(release_date) AS release_date, ROWTIME AS update_time FROM netflix_activity WHERE duration IS NOT NULL GROUP BY title EMIT CHANGES;"

echo "Creating daily engagement metrics stream..."
execute_ksql_query "CREATE TABLE IF NOT EXISTS daily_engagement_metrics WITH (KAFKA_TOPIC='netflix_daily_engagement_metrics', VALUE_FORMAT='JSON') AS SELECT FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp)), 'yyyy-MM-dd') AS view_date, COUNT_DISTINCT(user_id) AS total_unique_users, COUNT(*) AS total_views, SUM(duration) AS total_watch_time, AVG(CAST(duration AS DOUBLE)) AS avg_watch_time, LATEST_BY_OFFSET(title) AS most_watched_title, LATEST_BY_OFFSET(genres) AS most_watched_genre, CAST(FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp)), 'HH') AS INTEGER) AS peak_hour, ROWTIME AS update_time FROM netflix_activity WHERE duration IS NOT NULL GROUP BY FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp)), 'yyyy-MM-dd') EMIT CHANGES;"

echo "Queries have been executed. Check the Confluent Cloud console for results." 