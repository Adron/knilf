# Netflix UK Behavior Data Producer

This project streams Netflix UK user behavior data from a Kaggle dataset to Confluent Cloud using AVRO serialization and processes it using Flink SQL.

## Dataset Information

The data comes from the [Netflix Audience Behaviour - UK Movies](https://www.kaggle.com/datasets/vodclickstream/netflix-audience-behaviour-uk-movies) dataset on Kaggle. This dataset contains viewing behavior of UK Netflix users, including:

- Movie viewing sessions
- User interactions
- Movie metadata
- Temporal viewing patterns

### Data Fields

The dataset (`vodclickstream_uk_movies_03.csv`) contains the following fields:

- `datetime`: Timestamp of the user interaction (format: YYYY-MM-DD HH:MM:SS)
- `duration`: Viewing duration in seconds (0.0 for non-viewing interactions)
- `title`: Movie title
- `genres`: Comma-separated list of movie genres
- `release_date`: Movie's original release date (format: YYYY-MM-DD)
- `movie_id`: Unique identifier for the movie
- `user_id`: Unique identifier for the user

## Prerequisites

1. Python 3.8+
2. Kaggle account and API credentials (https://www.kaggle.com/account/login)
3. Confluent Cloud account with Schema Registry enabled (https://confluent.cloud)

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your environment variables by creating a `.env` file with:
```
# Confluent Cloud Configuration
BOOTSTRAP_SERVERS='your-bootstrap-servers'
SECURITY_PROTOCOL='SASL_SSL'
SASL_MECHANISMS='PLAIN'
SASL_USERNAME='your-api-key'
SASL_PASSWORD='your-api-secret'

# Schema Registry Configuration
SCHEMA_REGISTRY_URL='your-schema-registry-url'
SCHEMA_REGISTRY_API_KEY='your-schema-registry-api-key'
SCHEMA_REGISTRY_API_SECRET='your-schema-registry-api-secret'

# Kafka Topic
KAFKA_TOPIC='netflix_browsing_activity'
```

3. Set up Kaggle API credentials in `~/.kaggle/kaggle.json`

## Usage

1. Run the producer to stream data:
```bash
python netflix_producer.py
```

2. [Executing sample queries](sample_queries/executing-sample-queries.md) - Learn how to run and monitor the Flink SQL queries

## Data Flow

1. The producer script:
   - Downloads the dataset from Kaggle if not present
   - Reads the CSV data
   - Converts records to match the AVRO schema
   - Streams data to Confluent Cloud

2. Flink SQL queries:
   - Process the streaming data in real-time
   - Calculate viewing metrics
   - Store results in new Kafka topics

## Schema

The AVRO schema (`netflix_behavior_schema.avsc`) defines the message format with fields for:
- User identification
- Viewing timestamps
- Movie metadata
- Viewing duration
- Activity type

## Analytics

The Flink SQL queries provide two types of analytics:

1. Average Duration Analysis:
   - Average watch time per movie
   - Total views per movie

2. Daily Engagement Metrics:
   - Daily view counts per movie
   - Daily total watch time
   - Daily average watch time
   - Temporal viewing patterns
  
```
graph TD
   A[DPM API] --> B[Orders]
   A --> C[Products]
   B --> B1[all.json]
   B --> B2[ForcePrint]
   B --> B3[HandOff]
   B --> B4[Remake]
   B --> B5[RevertProduction]
   B --> B6[SetOrderCompleted]
   B --> B7[ToggleCustomerPresent]
   B --> B8[ToggleItemCompleted]
   B --> B9[ToggleItemsAvailable]
   B --> B10[ItemScan]
   C --> C1[products.json]
   C --> C2[ToggleItemAvailable]
```
