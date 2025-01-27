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