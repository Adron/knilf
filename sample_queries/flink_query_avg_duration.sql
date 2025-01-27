------------------ Statement 1: Create source table ------------------
CREATE TABLE netflix_activity (
    user_id STRING,
    event_time STRING,
    activity_type STRING,
    title_id STRING,
    duration INT,
    device_type STRING,
    location STRING,
    title STRING,
    genres STRING,
    release_date STRING,
    movie_id STRING,
    event_timestamp AS TO_TIMESTAMP(event_time),
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

------------------ Statement 2: Create target table ------------------
CREATE TABLE movie_duration_metrics (
    title STRING,
    total_views BIGINT,
    avg_duration DOUBLE,
    total_duration BIGINT,
    genres STRING,
    release_date STRING,
    update_time TIMESTAMP(3),
    PRIMARY KEY (title) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'netflix_movie_duration_metrics',
    'properties.bootstrap.servers' = 'BOOTSTRAP_SERVERS',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanisms' = 'PLAIN',
    'properties.sasl.username' = 'API_KEY',
    'properties.sasl.password' = 'API_SECRET',
    'key.format' = 'json',
    'value.format' = 'json'
);

------------------ Statement 3: Insert data ------------------
INSERT INTO movie_duration_metrics
SELECT
    title,
    COUNT(*) as total_views,
    AVG(CAST(duration AS DOUBLE)) as avg_duration,
    SUM(duration) as total_duration,
    MAX(genres) as genres,
    MAX(release_date) as release_date,
    CURRENT_TIMESTAMP as update_time
FROM netflix_activity
WHERE duration IS NOT NULL
GROUP BY title; 