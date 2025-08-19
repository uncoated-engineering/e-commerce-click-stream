# E-commerce Clickstream Simulation Pipeline

A comprehensive data engineering project that simulates and processes synthetic e-commerce clickstream data using Apache Kafka, Spark Structured Streaming, PostgreSQL, and Grafana.

## 🏗️ Architecture

```
[Data Generator] → [Kafka] → [Spark Streaming] → [PostgreSQL] → [Grafana]
```

- **Data Generator**: Python application that generates realistic e-commerce events (page views, cart additions, purchases)
- **Apache Kafka**: Message broker for real-time event streaming
- **Spark Structured Streaming**: Stream processing engine for real-time analytics
- **PostgreSQL**: Analytical database for storing processed metrics
- **Grafana**: Visualization dashboard for real-time monitoring

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.10+
- 8GB+ RAM recommended

### 1. Clone and Setup

```bash
git clone <repository-url>
cd e-commerce-click-stream

# Create Python virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt
```

### 2. Start Infrastructure

```bash
# Start all infrastructure services
./start_infrastructure.sh
```

This will start:
- Zookeeper & Kafka
- PostgreSQL with pre-configured schema
- Spark Master & Worker
- Grafana with PostgreSQL datasource

### 3. Start Data Pipeline

**Terminal 1 - Start Producer:**
```bash
./start_producer.sh
```

**Terminal 2 - Start Stream Processor:**
```bash
./start_processor.sh
```

### 4. Access Dashboards

- **Grafana**: http://localhost:3000 (admin/admin123)
- **Spark UI**: http://localhost:8080
- **PostgreSQL**: localhost:5432 (analytics_user/analytics_password)

## 📊 Data Model

### Raw Events Schema
```json
{
  "event_id": "uuid",
  "user_id": "uuid", 
  "event_type": "page_view | add_to_cart | purchase",
  "product_id": "uuid",
  "timestamp": "2025-08-19T14:05:22Z",
  "session_id": "uuid",
  "page_url": "/product/123",
  "user_agent": "Mozilla/5.0...",
  "ip_address": "192.168.1.1"
}
```

### Analytics Tables

1. **`analytics.raw_events`**: All raw events for audit trail
2. **`analytics.user_sessions`**: Aggregated session metrics per user
3. **`analytics.product_metrics`**: Product performance metrics
4. **`analytics.hourly_metrics`**: Time-series hourly aggregations
5. **`analytics.dashboard_metrics`**: Real-time KPI metrics

## 🔧 Configuration

Environment variables are configured in `.env`:

```bash
# Database
POSTGRES_HOST=localhost
POSTGRES_DB=ecommerce_analytics
POSTGRES_USER=analytics_user
POSTGRES_PASSWORD=analytics_password

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=clickstream.raw

# Producer Settings
PRODUCER_BATCH_SIZE=10
PRODUCER_SLEEP_INTERVAL=2
PRODUCER_MAX_USERS=1000
PRODUCER_MAX_PRODUCTS=500
```

## 📈 Metrics & Analytics

The pipeline calculates several key metrics:

### User Behavior Metrics
- Session duration
- Pages viewed per session
- Cart abandonment rate
- Conversion rate (page view → purchase)

### Product Metrics
- Product view counts
- Add-to-cart rates
- Purchase conversion rates
- Revenue per product

### Business KPIs
- Hourly traffic patterns
- Real-time conversion rates
- Average order value
- User engagement metrics

## 🛠️ Development

### Project Structure
```
├── producer/                 # Event generation
│   ├── data_generator.py    # Synthetic data generation
│   ├── models.py           # Data models
│   └── producer.py         # Kafka producer
├── processor/               # Stream processing
│   └── streaming_processor.py
├── db/                     # Database setup
│   └── init.sql           # Schema initialization
├── config/                # Configuration files
│   └── grafana/          # Grafana provisioning
├── docker-compose.yml     # Infrastructure setup
└── requirements.txt      # Python dependencies
```

### Running Tests
```bash
pytest tests/
```

### Code Quality
```bash
# Format code
black .

# Lint code  
flake8 .
```

## 🔍 Monitoring & Debugging

### Check Kafka Messages
```bash
# List topics
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consume messages
docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic clickstream.raw --from-beginning
```

### Check Database
```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U analytics_user -d ecommerce_analytics

# Query metrics
SELECT * FROM analytics.dashboard_metrics;
SELECT COUNT(*) FROM analytics.raw_events;
```

### Spark Monitoring
- Spark Master UI: http://localhost:8080
- Streaming queries progress in application logs

## 🚫 Troubleshooting

### Common Issues

**Kafka Connection Issues:**
```bash
# Restart Kafka services
docker-compose restart zookeeper kafka
```

**Spark Job Fails:**
- Check if all services are running: `docker-compose ps`
- Verify checkpoint directory permissions
- Check Spark logs: `docker-compose logs spark-master`

**Database Connection Issues:**
- Verify PostgreSQL is ready: `docker-compose logs postgres`
- Check network connectivity between containers

### Clean Restart
```bash
# Stop all services
docker-compose down -v

# Remove checkpoints
rm -rf processor/checkpoints/*

# Restart
./start_infrastructure.sh
```

## 🎯 Extension Ideas

- Add Apache Airflow for orchestration
- Implement real-time alerting with Kafka Connect
- Add machine learning models for recommendation engine
- Extend to multi-region deployment
- Add API layer for real-time queries
- Implement A/B testing metrics

## 📝 License

This project is for educational and portfolio demonstration purposes.

---

**Built with ❤️ for Data Engineering Excellence**