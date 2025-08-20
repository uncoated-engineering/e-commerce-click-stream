#!/bin/bash
set -e

echo "⚡ Starting Spark Streaming Processor..."

# Submit Spark job to the cluster
docker compose exec spark-master spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.0 \
    --master spark://spark-master:7077 \
    --executor-memory 1g \
    --driver-memory 1g \
    --conf spark.sql.streaming.checkpointLocation=/opt/spark/work-dir/checkpoints \
    /opt/spark/work-dir/streaming_processor.py