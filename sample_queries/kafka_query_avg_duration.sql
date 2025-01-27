-- Create source stream
CREATE STREAM IF NOT EXISTS netflix_activity (
    user_id STRING,
    timestamp STRING,
    activity_type STRING,
    title_id STRING,
    duration INTEGER,
    device_type STRING,
    location STRING,
    title STRING,
    genres STRING,
    release_date STRING,
    movie_id STRING
) WITH (
    KAFKA_TOPIC='netflix_browsing_activity',
    VALUE_FORMAT='AVRO',
    TIMESTAMP='timestamp',
    TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ss'
);

-- Create movie duration metrics table
CREATE TABLE IF NOT EXISTS movie_duration_metrics WITH (
    KAFKA_TOPIC='netflix_movie_duration_metrics',
    VALUE_FORMAT='JSON'
) AS
SELECT
    title,
    COUNT(*) AS total_views,
    AVG(CAST(duration AS DOUBLE)) AS avg_duration,
    SUM(duration) AS total_duration,
    LATEST_BY_OFFSET(genres) AS genres,
    LATEST_BY_OFFSET(release_date) AS release_date
FROM netflix_activity
WHERE duration IS NOT NULL
GROUP BY title
EMIT CHANGES; 