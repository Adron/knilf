-- Create a table from the Kafka topic with AVRO format
CREATE TABLE netflix_activity (
    user_id STRING,
    timestamp STRING,
    activity_type STRING,
    title_id STRING,
    duration INT,
    device_type STRING,
    location STRING,
    title STRING,
    genres STRING,
    release_date STRING,
    movie_id STRING,
    event_timestamp AS TO_TIMESTAMP(timestamp),
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' MINUTES
) WITH (
    'connector' = 'kafka',
    'topic' = 'netflix_browsing_activity',
    'properties.bootstrap.servers' = 'BOOTSTRAP_SERVERS',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanisms' = 'PLAIN',
    'properties.sasl.username' = 'API_KEY',
    'properties.sasl.password' = 'API_SECRET',
    'format' = 'avro-confluent',
    'avro-confluent.schema-registry.url' = 'SCHEMA_REGISTRY_URL',
    'avro-confluent.schema-registry.subject' = 'netflix_browsing_activity-value'
);

-- Create table for daily engagement metrics
CREATE TABLE daily_engagement_metrics (
    view_date DATE,
    total_unique_users BIGINT,
    total_views BIGINT,
    total_watch_time BIGINT,
    avg_watch_time DOUBLE,
    most_watched_title STRING,
    most_watched_genre STRING,
    peak_hour INT,
    update_time TIMESTAMP(3),
    PRIMARY KEY (view_date) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'netflix_daily_engagement_metrics',
    'properties.bootstrap.servers' = 'BOOTSTRAP_SERVERS',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanisms' = 'PLAIN',
    'properties.sasl.username' = 'API_KEY',
    'properties.sasl.password' = 'API_SECRET',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- Calculate daily engagement metrics
INSERT INTO daily_engagement_metrics
WITH hourly_stats AS (
    SELECT 
        DATE(event_timestamp) as view_date,
        EXTRACT(HOUR FROM event_timestamp) as hour_of_day,
        COUNT(*) as views_in_hour
    FROM netflix_activity
    GROUP BY 
        DATE(event_timestamp),
        EXTRACT(HOUR FROM event_timestamp)
),
peak_hours AS (
    SELECT 
        view_date,
        hour_of_day as peak_hour
    FROM (
        SELECT 
            view_date,
            hour_of_day,
            RANK() OVER (PARTITION BY view_date ORDER BY views_in_hour DESC) as rnk
        FROM hourly_stats
    )
    WHERE rnk = 1
),
genre_stats AS (
    SELECT 
        DATE(event_timestamp) as view_date,
        SUBSTRING_INDEX(genres, ',', 1) as primary_genre,
        COUNT(*) as genre_count
    FROM netflix_activity
    GROUP BY 
        DATE(event_timestamp),
        SUBSTRING_INDEX(genres, ',', 1)
),
most_watched_genres AS (
    SELECT 
        view_date,
        primary_genre as most_watched_genre
    FROM (
        SELECT 
            view_date,
            primary_genre,
            RANK() OVER (PARTITION BY view_date ORDER BY genre_count DESC) as rnk
        FROM genre_stats
    )
    WHERE rnk = 1
)
SELECT 
    DATE(a.event_timestamp) as view_date,
    COUNT(DISTINCT a.user_id) as total_unique_users,
    COUNT(*) as total_views,
    SUM(a.duration) as total_watch_time,
    AVG(CAST(a.duration AS DOUBLE)) as avg_watch_time,
    FIRST_VALUE(a.title) OVER (
        PARTITION BY DATE(a.event_timestamp) 
        ORDER BY COUNT(*) DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as most_watched_title,
    g.most_watched_genre,
    p.peak_hour,
    CURRENT_TIMESTAMP as update_time
FROM netflix_activity a
LEFT JOIN peak_hours p ON DATE(a.event_timestamp) = p.view_date
LEFT JOIN most_watched_genres g ON DATE(a.event_timestamp) = g.view_date
WHERE a.duration IS NOT NULL
GROUP BY 
    DATE(a.event_timestamp),
    g.most_watched_genre,
    p.peak_hour; 