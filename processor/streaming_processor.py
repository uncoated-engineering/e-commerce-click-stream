"""
Fixed Spark Structured Streaming job for processing e-commerce clickstream events.
Addresses schema mismatches with PostgreSQL database.
"""
import logging
import os
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, sum as spark_sum,
    avg, max as spark_max, min as spark_min, when, lit, 
    collect_list, size, first, last, unix_timestamp,
    current_timestamp, expr, regexp_replace, coalesce, approx_count_distinct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, DecimalType
)

from processor.schema_validator import SchemaValidator, validate_raw_events_schema, validate_session_metrics_schema, validate_hourly_metrics_schema


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
        
        # Define schema for incoming JSON events - matching PostgreSQL expectations
        self.event_schema = StructType([
            StructField("event_id", StringType(), False),  # Changed to NOT NULL
            StructField("user_id", StringType(), False),   # Changed to NOT NULL  
            StructField("event_type", StringType(), False), # Changed to NOT NULL
            StructField("product_id", StringType(), True),
            StructField("timestamp", StringType(), True),  # Allow null in parsing, handle later
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
    
    def validate_uuid(self, df: DataFrame, uuid_column: str) -> DataFrame:
        """Validate and clean UUID format to match PostgreSQL expectations."""
        # Remove any non-UUID characters and ensure proper format
        return df.withColumn(
            uuid_column,
            regexp_replace(col(uuid_column), "[^a-fA-F0-9-]", "")
        ).filter(
            # Basic UUID format validation (8-4-4-4-12)
            col(uuid_column).rlike("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$")
        )
    
    def parse_events(self, df: DataFrame) -> DataFrame:
        """Parse JSON events from Kafka and apply transformations."""
        # Parse JSON from Kafka value
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.event_schema).alias("event"),
            col("timestamp").alias("kafka_timestamp")
        ).select("event.*", "kafka_timestamp")
        
        # Convert timestamp string to timestamp type and add created_at
        # Handle cases where timestamp might be missing/null by using current timestamp
        transformed_df = parsed_df.withColumn(
            "timestamp", 
            when(
                col("timestamp").isNotNull() & (col("timestamp") != ""),
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            ).otherwise(current_timestamp())
        ).withColumn(
            "processing_time", 
            current_timestamp()
        ).withColumn(
            "created_at",  # Add created_at column for PostgreSQL
            current_timestamp()
        )
        
        # Validate UUID columns
        transformed_df = self.validate_uuid(transformed_df, "event_id")
        transformed_df = self.validate_uuid(transformed_df, "user_id")
        
        # Validate product_id and session_id only if not null
        transformed_df = transformed_df.withColumn(
            "product_id",
            when(col("product_id").isNotNull(), 
                 regexp_replace(col("product_id"), "[^a-fA-F0-9-]", ""))
            .otherwise(col("product_id"))
        ).withColumn(
            "session_id",
            when(col("session_id").isNotNull(), 
                 regexp_replace(col("session_id"), "[^a-fA-F0-9-]", ""))
            .otherwise(col("session_id"))
        )
        
        return transformed_df
    
    def write_raw_events(self, df: DataFrame) -> None:
        """Write raw events to PostgreSQL for audit trail."""
        def write_to_postgres(batch_df, batch_id):
            try:
                if batch_df.count() > 0:
                    # Validate schema before writing
                    is_compatible, issues = validate_raw_events_schema(batch_df)
                    if not is_compatible:
                        logger.warning(f"Schema validation issues found for raw_events batch {batch_id}:")
                        fixes = SchemaValidator.suggest_fixes(issues)
                        for fix in fixes:
                            logger.warning(f"  Fix: {fix['issue']} -> {fix['fix']}")
                    
                    # Apply schema fixes to prevent write errors
                    # Fix 1: Convert timestamp to string to match PostgreSQL TEXT type
                    batch_df = batch_df.withColumn("timestamp", col("timestamp").cast("string"))
                    
                    # Select only columns that exist in PostgreSQL table
                    postgres_df = batch_df.select(
                        col("event_id").cast("string"),
                        col("user_id").cast("string"), 
                        col("event_type").cast("string"),
                        col("product_id").cast("string"),
                        col("timestamp"),  # Now properly cast to string
                        col("session_id").cast("string"),
                        col("page_url").cast("string"),
                        col("user_agent").cast("string"),
                        col("ip_address").cast("string"),
                        col("created_at")
                    )
                    
                    postgres_df.write \
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
            .trigger(processingTime="90 seconds") \
            .start()
        
        return query
    
    def calculate_session_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate user session metrics with proper data types for PostgreSQL."""
        session_metrics = df.groupBy("session_id", "user_id") \
            .agg(
                spark_min("timestamp").alias("start_time"),
                spark_max("timestamp").alias("end_time"),
                count("*").alias("total_events"),
                spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
                spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_events"),
                spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
                current_timestamp().alias("updated_at"),
                current_timestamp().alias("created_at")  # Add created_at
            ) \
            .withColumn(
                "session_duration_minutes",
                ((unix_timestamp("end_time") - unix_timestamp("start_time")) / 60).cast("integer")
            ) \
            .withColumn(
                "converted",
                when(col("purchases") > 0, lit(True)).otherwise(lit(False))
            ) \
            .withColumn(
                "total_purchase_amount",
                (col("purchases") * 25.99).cast(DecimalType(10, 2))  # Match PostgreSQL DECIMAL(10,2)
            )
        
        return session_metrics
    
    def write_session_metrics(self, df: DataFrame) -> None:
        """Write session metrics to PostgreSQL."""
        def upsert_session_metrics(batch_df, batch_id):
            try:
                if batch_df.count() > 0:
                    # Validate schema before writing
                    is_compatible, issues = validate_session_metrics_schema(batch_df)
                    if not is_compatible:
                        logger.warning(f"Schema validation issues found for user_sessions batch {batch_id}:")
                        fixes = SchemaValidator.suggest_fixes(issues)
                        for fix in fixes:
                            logger.warning(f"  Fix: {fix['issue']} -> {fix['fix']}")
                    
                    # Apply schema fixes to prevent write errors
                    # Fix 1: Convert start_time to string to match PostgreSQL TEXT type
                    batch_df = batch_df.withColumn("start_time", col("start_time").cast("string"))
                    
                    # Select and cast columns to match PostgreSQL schema
                    postgres_df = batch_df.select(
                        col("session_id").cast("string"),
                        col("user_id").cast("string"),
                        col("start_time"),  # Now properly cast to string
                        col("end_time").cast("timestamp"),  # Ensure proper timestamp type
                        col("session_duration_minutes").cast("integer"),
                        col("page_views").cast("integer"),
                        col("add_to_cart_events").cast("integer"), 
                        col("purchases").cast("integer"),
                        col("total_purchase_amount"),
                        col("converted").cast("boolean"),
                        col("created_at"),
                        col("updated_at")
                    )
                    
                    postgres_df.write \
                        .format("jdbc") \
                        .option("url", self.postgres_url) \
                        .option("dbtable", "analytics.user_sessions") \
                        .option("user", self.postgres_user) \
                        .option("password", self.postgres_password) \
                        .option("driver", "org.postgresql.Driver") \
                        .mode("overwrite") \
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
        """Calculate hourly aggregated metrics with proper data types."""
        hourly_metrics = df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "1 hour").alias("hour_window")
            ) \
            .agg(
                count("*").alias("total_events"),
                approx_count_distinct("user_id").alias("unique_users"),
                spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
                spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_additions"),
                spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            ) \
            .select(
                col("hour_window.start").alias("hour_timestamp"),
                col("total_events").cast("integer"),
                col("unique_users").cast("integer"),
                col("page_views").cast("integer"),
                col("cart_additions").cast("integer"),
                col("purchases").cast("integer"),
                # Safe division with null handling and proper decimal type
                when(col("page_views") > 0, 
                     (col("purchases").cast("double") / col("page_views").cast("double") * 100))
                .otherwise(lit(0.0))
                .cast(DecimalType(5, 4))
                .alias("conversion_rate"),
                (col("purchases") * 25.99).cast(DecimalType(12, 2)).alias("revenue")
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
    
    def update_dashboard_metrics(self, df: DataFrame) -> None:
        """Update real-time dashboard metrics."""
        def update_dashboard_batch(batch_df, batch_id):
            try:
                if batch_df.count() > 0:
                    # Calculate current metrics
                    metrics = batch_df.agg(
                        expr("count(distinct user_id)").alias("total_users"),
                        expr("count(distinct session_id)").alias("total_sessions"),
                        avg(when(col("event_type") == "purchase", 1.0).otherwise(0.0)).alias("conversion_rate"),
                        avg("session_duration_minutes").alias("avg_session_duration")
                    ).collect()[0]
                    
                    # Create dashboard updates DataFrame
                    dashboard_updates = self.spark.createDataFrame([
                        ("total_users", float(metrics["total_users"] or 0), "Total Active Users"),
                        ("total_sessions", float(metrics["total_sessions"] or 0), "Total Sessions"), 
                        ("conversion_rate", float(metrics["conversion_rate"] or 0) * 100, "Overall Conversion Rate (%)"),
                        ("avg_session_duration", float(metrics["avg_session_duration"] or 0), "Average Session Duration (minutes)")
                    ], ["metric_key", "metric_value", "metric_label"])
                    
                    dashboard_updates = dashboard_updates.withColumn(
                        "last_updated", current_timestamp()
                    ).withColumn(
                        "metric_value", col("metric_value").cast(DecimalType(15, 4))
                    )
                    
                    # Write to dashboard metrics (using overwrite for simplicity)
                    dashboard_updates.write \
                        .format("jdbc") \
                        .option("url", self.postgres_url) \
                        .option("dbtable", "analytics.dashboard_metrics") \
                        .option("user", self.postgres_user) \
                        .option("password", self.postgres_password) \
                        .option("driver", "org.postgresql.Driver") \
                        .mode("overwrite") \
                        .save()
                    
                    logger.info(f"Batch {batch_id}: Updated dashboard metrics")
                    
            except Exception as e:
                logger.error(f"Failed to update dashboard metrics batch {batch_id}: {e}")
        
        # Aggregate session metrics for dashboard updates
        dashboard_df = df.groupBy("session_id").agg(
            first("user_id").alias("user_id"),
            first("event_type").alias("event_type"),
            ((unix_timestamp(spark_max("timestamp")) - unix_timestamp(spark_min("timestamp"))) / 60).alias("session_duration_minutes")
        )
        
        query = dashboard_df.writeStream \
            .outputMode("update") \
            .foreachBatch(update_dashboard_batch) \
            .option("checkpointLocation", f"{self.checkpoint_location}/dashboard_metrics") \
            .trigger(processingTime="3 minutes") \
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
            
            # Update dashboard metrics
            dashboard_query = self.update_dashboard_metrics(parsed_events)
            
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