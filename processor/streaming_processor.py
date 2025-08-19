"""
Spark Structured Streaming job for processing e-commerce clickstream events.
"""
import logging
import os
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, sum as spark_sum,
    avg, max as spark_max, min as spark_min, when, lit, 
    collect_list, size, first, last, unix_timestamp,
    current_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ClickstreamProcessor:
    """Processes e-commerce clickstream events using Spark Structured Streaming."""
    
    def __init__(
        self,
        app_name: str = "EcommerceClickstreamProcessor",
        kafka_bootstrap_servers: str = "kafka:29092",
        kafka_topic: str = "clickstream.raw",
        postgres_url: str = "jdbc:postgresql://postgres:5432/ecommerce_analytics",
        postgres_user: str = "analytics_user",
        postgres_password: str = "analytics_password",
        checkpoint_location: str = "/opt/spark/work-dir/checkpoints",
    ):
        self.app_name = app_name
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.postgres_url = postgres_url
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.checkpoint_location = checkpoint_location
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Define schema for incoming JSON events
        self.event_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("ip_address", StringType(), True),
        ])
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        try:
            spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location) \
                .config("spark.jars.packages", 
                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                       "org.postgresql:postgresql:42.7.0") \
                .getOrCreate()
            
            # Set log level to reduce noise
            spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"Created Spark session: {self.app_name}")
            return spark
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise
    
    def read_kafka_stream(self) -> DataFrame:
        """Read streaming data from Kafka."""
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            logger.info(f"Connected to Kafka topic: {self.kafka_topic}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read from Kafka: {e}")
            raise
    
    def parse_events(self, df: DataFrame) -> DataFrame:
        """Parse JSON events from Kafka and apply transformations."""
        # Parse JSON from Kafka value
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.event_schema).alias("event"),
            col("timestamp").alias("kafka_timestamp")
        ).select("event.*", "kafka_timestamp")
        
        # Convert timestamp string to timestamp type
        transformed_df = parsed_df.withColumn(
            "timestamp", 
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        ).withColumn(
            "processing_time", 
            current_timestamp()
        )
        
        return transformed_df
    
    def write_raw_events(self, df: DataFrame) -> None:
        """Write raw events to PostgreSQL for audit trail."""
        def write_to_postgres(batch_df, batch_id):
            try:
                if batch_df.count() > 0:
                    batch_df.write \
                        .format("jdbc") \
                        .option("url", self.postgres_url) \
                        .option("dbtable", "analytics.raw_events") \
                        .option("user", self.postgres_user) \
                        .option("password", self.postgres_password) \
                        .option("driver", "org.postgresql.Driver") \
                        .mode("append") \
                        .save()
                    
                    logger.info(f"Batch {batch_id}: Wrote {batch_df.count()} raw events")
            except Exception as e:
                logger.error(f"Failed to write raw events batch {batch_id}: {e}")
        
        query = df.writeStream \
            .outputMode("append") \
            .foreachBatch(write_to_postgres) \
            .option("checkpointLocation", f"{self.checkpoint_location}/raw_events") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def calculate_session_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate user session metrics."""
        session_metrics = df.groupBy("session_id", "user_id") \
            .agg(
                spark_min("timestamp").alias("start_time"),
                spark_max("timestamp").alias("end_time"),
                count("*").alias("total_events"),
                spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
                spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_events"),
                spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
                current_timestamp().alias("updated_at")
            ) \
            .withColumn(
                "session_duration_minutes",
                (unix_timestamp("end_time") - unix_timestamp("start_time")) / 60
            ) \
            .withColumn(
                "converted",
                when(col("purchases") > 0, lit(True)).otherwise(lit(False))
            ) \
            .withColumn(
                "total_purchase_amount",
                col("purchases") * 25.99  # Simplified: assume average order value
            )
        
        return session_metrics
    
    def write_session_metrics(self, df: DataFrame) -> None:
        """Write session metrics to PostgreSQL with upsert logic."""
        def upsert_session_metrics(batch_df, batch_id):
            try:
                if batch_df.count() > 0:
                    # Create temporary table
                    batch_df.createOrReplaceTempView("temp_session_metrics")
                    
                    # Use PostgreSQL UPSERT (ON CONFLICT DO UPDATE)
                    upsert_query = """
                    INSERT INTO analytics.user_sessions (
                        session_id, user_id, start_time, end_time, session_duration_minutes,
                        page_views, add_to_cart_events, purchases, total_purchase_amount, 
                        converted, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (session_id) DO UPDATE SET
                        end_time = EXCLUDED.end_time,
                        session_duration_minutes = EXCLUDED.session_duration_minutes,
                        page_views = EXCLUDED.page_views,
                        add_to_cart_events = EXCLUDED.add_to_cart_events,
                        purchases = EXCLUDED.purchases,
                        total_purchase_amount = EXCLUDED.total_purchase_amount,
                        converted = EXCLUDED.converted,
                        updated_at = EXCLUDED.updated_at
                    """
                    
                    # For simplicity, use overwrite mode (in production, use proper upsert)
                    batch_df.write \
                        .format("jdbc") \
                        .option("url", self.postgres_url) \
                        .option("dbtable", "analytics.user_sessions") \
                        .option("user", self.postgres_user) \
                        .option("password", self.postgres_password) \
                        .option("driver", "org.postgresql.Driver") \
                        .mode("append") \
                        .save()
                    
                    logger.info(f"Batch {batch_id}: Updated {batch_df.count()} session metrics")
                    
            except Exception as e:
                logger.error(f"Failed to write session metrics batch {batch_id}: {e}")
        
        query = df.writeStream \
            .outputMode("update") \
            .foreachBatch(upsert_session_metrics) \
            .option("checkpointLocation", f"{self.checkpoint_location}/session_metrics") \
            .trigger(processingTime="1 minute") \
            .start()
        
        return query
    
    def calculate_hourly_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate hourly aggregated metrics."""
        hourly_metrics = df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "1 hour").alias("hour_window")
            ) \
            .agg(
                count("*").alias("total_events"),
                expr("count(distinct user_id)").alias("unique_users"),
                spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
                spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_additions"),
                spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            ) \
            .select(
                col("hour_window.start").alias("hour_timestamp"),
                col("total_events"),
                col("unique_users"),
                col("page_views"),
                col("cart_additions"),
                col("purchases"),
                (col("purchases").cast("double") / col("page_views").cast("double") * 100).alias("conversion_rate"),
                (col("purchases") * 25.99).alias("revenue")  # Simplified revenue calculation
            )
        
        return hourly_metrics
    
    def write_hourly_metrics(self, df: DataFrame) -> None:
        """Write hourly metrics to PostgreSQL."""
        def write_hourly_batch(batch_df, batch_id):
            try:
                if batch_df.count() > 0:
                    batch_df.write \
                        .format("jdbc") \
                        .option("url", self.postgres_url) \
                        .option("dbtable", "analytics.hourly_metrics") \
                        .option("user", self.postgres_user) \
                        .option("password", self.postgres_password) \
                        .option("driver", "org.postgresql.Driver") \
                        .mode("append") \
                        .save()
                    
                    logger.info(f"Batch {batch_id}: Wrote {batch_df.count()} hourly metrics")
            except Exception as e:
                logger.error(f"Failed to write hourly metrics batch {batch_id}: {e}")
        
        query = df.writeStream \
            .outputMode("append") \
            .foreachBatch(write_hourly_batch) \
            .option("checkpointLocation", f"{self.checkpoint_location}/hourly_metrics") \
            .trigger(processingTime="5 minutes") \
            .start()
        
        return query
    
    def run(self) -> None:
        """Run the complete streaming pipeline."""
        try:
            logger.info("Starting clickstream processor...")
            
            # Read from Kafka
            raw_stream = self.read_kafka_stream()
            
            # Parse events
            parsed_events = self.parse_events(raw_stream)
            
            # Write raw events for audit
            raw_events_query = self.write_raw_events(parsed_events)
            
            # Calculate and write session metrics
            session_metrics = self.calculate_session_metrics(parsed_events)
            session_query = self.write_session_metrics(session_metrics)
            
            # Calculate and write hourly metrics
            hourly_metrics = self.calculate_hourly_metrics(parsed_events)
            hourly_query = self.write_hourly_metrics(hourly_metrics)
            
            logger.info("All streaming queries started successfully")
            
            # Wait for termination
            raw_events_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in streaming pipeline: {e}")
            raise
        finally:
            self.spark.stop()


def main():
    """Main function to run the processor."""
    # Get configuration from environment variables
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "clickstream.raw")
    postgres_host = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_db = os.getenv("POSTGRES_DB", "ecommerce_analytics")
    postgres_user = os.getenv("POSTGRES_USER", "analytics_user")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "analytics_password")
    
    postgres_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    
    # Create and run processor
    processor = ClickstreamProcessor(
        kafka_bootstrap_servers=kafka_servers,
        kafka_topic=kafka_topic,
        postgres_url=postgres_url,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
    )
    
    processor.run()


if __name__ == "__main__":
    main()