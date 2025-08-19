#!/bin/bash
set -e

echo "🚀 Starting E-commerce Clickstream Pipeline..."

# Load environment variables
if [ -f .env ]; then
    source .env
fi

# Create necessary directories
mkdir -p processor/checkpoints

# Start infrastructure services
echo "📦 Starting infrastructure services..."
docker-compose up -d zookeeper kafka postgres grafana

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check Kafka is ready
echo "🔍 Checking Kafka connectivity..."
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Create Kafka topic if it doesn't exist
echo "📡 Creating Kafka topic..."
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic clickstream.raw --partitions 3 --replication-factor 1 --if-not-exists

# Start Spark services
echo "⚡ Starting Spark services..."
docker-compose up -d spark-master spark-worker

# Wait for Spark to be ready
echo "⏳ Waiting for Spark to be ready..."
sleep 20

echo "✅ Infrastructure is ready!"
echo ""
echo "🌐 Services are available at:"
echo "  - Grafana: http://localhost:3000 (admin/admin123)"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - PostgreSQL: localhost:5432"
echo ""
echo "To start the data pipeline:"
echo "  1. Run producer: python -m producer.producer"
echo "  2. Run processor: docker-compose exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.0 /opt/spark/work-dir/streaming_processor.py"