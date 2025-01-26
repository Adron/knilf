import os
import json
import time
from dotenv import load_dotenv
import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import kaggle

# Load environment variables
load_dotenv()

# Download dataset from Kaggle
def download_dataset():
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(
        'vodclickstream/netflix-audience-behaviour-uk-movies',
        path='./data',
        unzip=True
    )

# Load AVRO schema
def load_avro_schema():
    with open('netflix_behavior_schema.avsc', 'r') as f:
        return f.read()

# Configure Kafka producer
def get_kafka_producer(schema_registry_client):
    schema_str = load_avro_schema()
    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        lambda x, ctx: x
    )

    producer_config = {
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('SECURITY_PROTOCOL'),
        'sasl.mechanisms': os.getenv('SASL_MECHANISMS'),
        'sasl.username': os.getenv('SASL_USERNAME'),
        'sasl.password': os.getenv('SASL_PASSWORD'),
        'key.serializer': StringSerializer(),
        'value.serializer': avro_serializer
    }

    return SerializingProducer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def main():
    # Download dataset if not exists
    if not os.path.exists('./data/vodclickstream_uk_movies_03.csv'):
        print("Downloading dataset from Kaggle...")
        download_dataset()

    # Initialize Schema Registry client
    schema_registry_conf = {
        'url': os.getenv('SCHEMA_REGISTRY_URL'),
        'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Initialize producer
    producer = get_kafka_producer(schema_registry_client)
    topic = os.getenv('KAFKA_TOPIC')

    # Read and process the dataset
    df = pd.read_csv('./data/vodclickstream_uk_movies_03.csv', index_col=0)
    
    print(f"Starting to stream {len(df)} records to topic {topic}...")
    
    # Stream each record
    for _, row in df.iterrows():
        record = {
            'USER_ID': str(row['user_id']),
            'TIMESTAMP': str(row['datetime']),
            'ACTIVITY_TYPE': 'view',  # Since this is viewing data
            'TITLE_ID': str(row['movie_id']),
            'DURATION': int(float(row['duration'])) if pd.notna(row['duration']) else None,
            'DEVICE_TYPE': None,  # Not available in the dataset
            'LOCATION': None,  # Not available in the dataset
            'TITLE': str(row['title']),
            'GENRES': str(row['genres']),
            'RELEASE_DATE': str(row['release_date']),
            'MOVIE_ID': str(row['movie_id'])
        }
        
        try:
            producer.produce(
                topic=topic,
                key=str(row['user_id']),
                value=record,
                on_delivery=delivery_report
            )
            producer.poll(0)  # Trigger delivery reports
            
            # Add small delay to avoid overwhelming the broker
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Error producing record: {e}")
    
    # Flush any remaining messages
    producer.flush()
    print("Finished streaming all records.")

if __name__ == "__main__":
    main() 