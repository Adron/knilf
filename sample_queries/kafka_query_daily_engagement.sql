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

-- Create daily engagement metrics table
CREATE TABLE IF NOT EXISTS daily_engagement_metrics WITH (
    KAFKA_TOPIC='netflix_daily_engagement_metrics',
    KEY_FORMAT='JSON',
    VALUE_FORMAT='JSON'
) AS 
SELECT 
    FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp)), 'yyyy-MM-dd') AS view_date,
    COUNT_DISTINCT(user_id) AS total_unique_users,
    COUNT(*) AS total_views,
    SUM(duration) AS total_watch_time,
    AVG(CAST(duration AS DOUBLE)) AS avg_watch_time,
    LATEST_BY_OFFSET(title) AS most_watched_title,
    LATEST_BY_OFFSET(genres) AS most_watched_genre,
    FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp)), 'HH') AS hour_of_day
FROM netflix_activity 
WHERE duration IS NOT NULL
GROUP BY FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp)), 'yyyy-MM-dd'),
         FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp)), 'HH')
EMIT CHANGES; 