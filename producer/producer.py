"""
Kafka producer for streaming e-commerce clickstream events.
"""
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .data_generator import EcommerceDataGenerator
from .models import ClickstreamEvent, UserSession


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ClickstreamProducer:
    """Produces synthetic e-commerce clickstream events to Kafka."""
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "clickstream.raw",
        max_users: int = 1000,
        max_products: int = 500,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.data_generator = EcommerceDataGenerator(max_users, max_products)
        
        # Active user sessions
        self.active_sessions: Dict[str, UserSession] = {}
        self.session_events: Dict[str, List[str]] = {}
        
        # Initialize Kafka producer
        self.producer = self._create_producer()
        
    def _create_producer(self) -> KafkaProducer:
        """Create and configure Kafka producer."""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                batch_size=16384,
                linger_ms=10,
                retries=3,
                acks='all',
            )
            logger.info(f"Successfully connected to Kafka at {self.bootstrap_servers}")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise
    
    def _should_end_session(self, session: UserSession) -> bool:
        """Determine if a user session should end."""
        session_duration = datetime.utcnow() - session.start_time
        
        # End session after 5-30 minutes with some probability
        if session_duration > timedelta(minutes=5):
            # Probability increases with session duration
            minutes = session_duration.total_seconds() / 60
            end_probability = min(0.3, 0.05 + (minutes - 5) * 0.02)
            return random.random() < end_probability
        
        return False
    
    def _create_new_session(self) -> UserSession:
        """Create a new user session."""
        user_id = self.data_generator.generate_user_id()
        session = UserSession.create(user_id)
        
        self.active_sessions[session.session_id] = session
        self.session_events[session.session_id] = []
        
        logger.info(f"Created new session {session.session_id} for user {user_id}")
        return session
    
    def _end_session(self, session_id: str) -> None:
        """End a user session."""
        if session_id in self.active_sessions:
            session = self.active_sessions.pop(session_id)
            self.session_events.pop(session_id, None)
            logger.info(f"Ended session {session_id} for user {session.user_id}")
    
    def _generate_event(self, session: UserSession) -> ClickstreamEvent:
        """Generate a single clickstream event for a session."""
        session_history = self.session_events.get(session.session_id, [])
        event_probs = self.data_generator.get_event_type_probabilities(session_history)
        
        # Select event type based on probabilities
        event_types = list(event_probs.keys())
        weights = list(event_probs.values())
        event_type = random.choices(event_types, weights=weights)[0]
        
        # Generate product ID if needed
        product_id = None
        if event_type in ["add_to_cart", "purchase"]:
            product_id = self.data_generator.generate_product_id()
        elif event_type == "page_view" and random.random() < 0.6:
            product_id = self.data_generator.generate_product_id()
        
        # Generate page URL
        page_url = self.data_generator.generate_page_url(event_type, product_id)
        
        # Create the event
        event = ClickstreamEvent.create(
            user_id=session.user_id,
            event_type=event_type,
            session_id=session.session_id,
            product_id=product_id,
            page_url=page_url,
            user_agent=self.data_generator.generate_user_agent(),
            ip_address=self.data_generator.generate_ip_address(),
        )
        
        # Update session state
        session.current_page = page_url
        if event_type == "add_to_cart" and product_id:
            session.cart_items.append(product_id)
        elif event_type == "purchase":
            session.cart_items.clear()  # Clear cart after purchase
        
        # Track event in session history
        self.session_events[session.session_id].append(event_type)
        
        return event
    
    def _send_event(self, event: ClickstreamEvent) -> None:
        """Send event to Kafka topic."""
        try:
            future = self.producer.send(
                self.topic,
                key=event.user_id,
                value=event.to_dict()
            )
            
            # Add callback for success/failure
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
    
    def _on_send_success(self, record_metadata):
        """Callback for successful message send."""
        logger.debug(
            f"Event sent successfully to {record_metadata.topic} "
            f"partition {record_metadata.partition} offset {record_metadata.offset}"
        )
    
    def _on_send_error(self, exception):
        """Callback for failed message send."""
        logger.error(f"Failed to send event: {exception}")
    
    def produce_batch(self, batch_size: int = 10) -> int:
        """Produce a batch of events."""
        events_sent = 0
        
        try:
            # Ensure we have some active sessions
            while len(self.active_sessions) < min(10, batch_size):
                self._create_new_session()
            
            # Generate events from active sessions
            sessions_to_end = []
            
            for session_id, session in list(self.active_sessions.items()):
                if events_sent >= batch_size:
                    break
                
                # Check if session should end
                if self._should_end_session(session):
                    sessions_to_end.append(session_id)
                    continue
                
                # Generate and send event
                event = self._generate_event(session)
                self._send_event(event)
                events_sent += 1
                
                logger.info(
                    f"Sent {event.event_type} event for user {event.user_id} "
                    f"on product {event.product_id}"
                )
            
            # End sessions that should be ended
            for session_id in sessions_to_end:
                self._end_session(session_id)
            
            # Flush producer to ensure messages are sent
            self.producer.flush(timeout=10)
            
        except Exception as e:
            logger.error(f"Error producing batch: {e}")
        
        return events_sent
    
    def run(
        self,
        batch_size: int = 10,
        sleep_interval: float = 2.0,
        max_batches: Optional[int] = None,
    ) -> None:
        """Run the producer continuously."""
        logger.info(
            f"Starting clickstream producer - batch_size: {batch_size}, "
            f"interval: {sleep_interval}s, max_batches: {max_batches}"
        )
        
        batch_count = 0
        total_events = 0
        
        try:
            while max_batches is None or batch_count < max_batches:
                start_time = time.time()
                
                events_sent = self.produce_batch(batch_size)
                total_events += events_sent
                batch_count += 1
                
                processing_time = time.time() - start_time
                
                logger.info(
                    f"Batch {batch_count}: {events_sent} events sent "
                    f"(total: {total_events}) in {processing_time:.2f}s. "
                    f"Active sessions: {len(self.active_sessions)}"
                )
                
                if sleep_interval > processing_time:
                    time.sleep(sleep_interval - processing_time)
                    
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping producer...")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            self.close()
    
    def close(self) -> None:
        """Close the producer and clean up resources."""
        if self.producer:
            self.producer.close(timeout=10)
        logger.info("Producer closed")


def main():
    """Main function to run the producer."""
    # Get configuration from environment variables
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "clickstream.raw")
    batch_size = int(os.getenv("PRODUCER_BATCH_SIZE", "10"))
    sleep_interval = float(os.getenv("PRODUCER_SLEEP_INTERVAL", "2.0"))
    max_users = int(os.getenv("PRODUCER_MAX_USERS", "1000"))
    max_products = int(os.getenv("PRODUCER_MAX_PRODUCTS", "500"))
    
    # Create and run producer
    producer = ClickstreamProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        max_users=max_users,
        max_products=max_products,
    )
    
    producer.run(
        batch_size=batch_size,
        sleep_interval=sleep_interval,
    )


if __name__ == "__main__":
    main()