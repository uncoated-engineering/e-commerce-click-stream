"""
E-commerce clickstream event data models with proper UUID and timestamp handling.
"""
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Optional, List
import uuid
import json


@dataclass
class ClickstreamEvent:
    """Represents a single clickstream event with proper data types."""
    event_id: str
    user_id: str
    event_type: str  # page_view, add_to_cart, purchase
    product_id: Optional[str]
    timestamp: datetime
    session_id: str
    page_url: Optional[str] = None
    user_agent: Optional[str] = None
    ip_address: Optional[str] = None
    
    @classmethod
    def create(
        cls,
        user_id: str,
        event_type: str,
        session_id: str,
        product_id: Optional[str] = None,
        page_url: Optional[str] = None,
        user_agent: Optional[str] = None,
        ip_address: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> "ClickstreamEvent":
        """Factory method to create a new clickstream event with proper data types."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        return cls(
            event_id=str(uuid.uuid4()),
            user_id=user_id,
            event_type=event_type,
            product_id=product_id,
            timestamp=timestamp,
            session_id=session_id,
            page_url=page_url,
            user_agent=user_agent,
            ip_address=ip_address,
        )
    
    def to_json(self) -> str:
        """Convert the event to JSON string with ISO format timestamp."""
        data = self.to_dict()
        return json.dumps(data)
    
    def to_dict(self) -> dict:
        """Convert the event to dictionary with ISO format timestamp."""
        data = asdict(self)
        # Convert datetime to ISO format string for Kafka serialization
        data['timestamp'] = self.timestamp.isoformat()
        return data


@dataclass
class UserSession:
    """Represents a user session for tracking user behavior."""
    session_id: str
    user_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    current_page: Optional[str] = None
    cart_items: List[str] = field(default_factory=list)
    event_count: int = 0
    page_view_count: int = 0
    add_to_cart_count: int = 0
    purchase_count: int = 0
    
    @classmethod
    def create(cls, user_id: str) -> "UserSession":
        """Create a new user session with proper UUID."""
        return cls(
            session_id=str(uuid.uuid4()),
            user_id=user_id,
            start_time=datetime.now(timezone.utc),
            cart_items=[],
            event_count=0,
            page_view_count=0,
            add_to_cart_count=0,
            purchase_count=0,
        )
    
    def add_event(self, event_type: str) -> None:
        """Track event counts for the session."""
        self.event_count += 1
        if event_type == "page_view":
            self.page_view_count += 1
        elif event_type == "add_to_cart":
            self.add_to_cart_count += 1
        elif event_type == "purchase":
            self.purchase_count += 1
    
    def get_duration_seconds(self) -> int:
        """Get session duration in seconds."""
        end = self.end_time or datetime.now(timezone.utc)
        duration = (end - self.start_time).total_seconds()
        return max(0, int(duration))
    
    def close(self) -> None:
        """Close the session."""
        if self.end_time is None:
            self.end_time = datetime.now(timezone.utc)