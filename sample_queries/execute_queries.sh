#!/bin/bash

# Load environment variables
source ../.env

# Create temporary SQL files with environment variables replaced
for file in *.sql; do
    echo "Processing $file..."
    sed -e "s|BOOTSTRAP_SERVERS|$BOOTSTRAP_SERVERS|g" \
        -e "s|SASL_USERNAME|$SASL_USERNAME|g" \
        -e "s|SASL_PASSWORD|$SASL_PASSWORD|g" \
        -e "s|SCHEMA_REGISTRY_URL|$SCHEMA_REGISTRY_URL|g" \
        -e "s|API_KEY|$SASL_USERNAME|g" \
        -e "s|API_SECRET|$SASL_PASSWORD|g" \
        "$file" > "${file}.tmp"
done

# Start Flink SQL Client with our configuration
FLINK_HOME=/opt/homebrew/Cellar/apache-flink/1.20.0/libexec
$FLINK_HOME/bin/sql-client.sh embedded \
    -j $FLINK_HOME/lib/flink-json-1.20.0.jar \
    -j $FLINK_HOME/lib/flink-sql-connector-kafka-1.20.0.jar \
    -j $FLINK_HOME/lib/flink-sql-avro-1.20.0.jar \
    --configuration "state.backend: rocksdb" \
    --configuration "state.checkpoints.dir: file:///tmp/flink-checkpoints" \
    --configuration "execution.checkpointing.interval: 30s" \
    --init "flink_query_avg_duration.sql.tmp" \
    --init "flink_query_daily_engagement.sql.tmp"

# Clean up temporary files
rm -f *.sql.tmp 