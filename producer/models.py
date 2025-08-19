"""
E-commerce clickstream event data models.
"""
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional
import uuid
import json


@dataclass
class ClickstreamEvent:
    """Represents a single clickstream event."""
    event_id: str
    user_id: str
    event_type: str  # page_view, add_to_cart, purchase
    product_id: Optional[str]
    timestamp: str
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
    ) -> "ClickstreamEvent":
        """Factory method to create a new clickstream event."""
        return cls(
            event_id=str(uuid.uuid4()),
            user_id=user_id,
            event_type=event_type,
            product_id=product_id,
            timestamp=datetime.utcnow().isoformat() + "Z",
            session_id=session_id,
            page_url=page_url,
            user_agent=user_agent,
            ip_address=ip_address,
        )
    
    def to_json(self) -> str:
        """Convert the event to JSON string."""
        return json.dumps(asdict(self))
    
    def to_dict(self) -> dict:
        """Convert the event to dictionary."""
        return asdict(self)


@dataclass
class UserSession:
    """Represents a user session for tracking user behavior."""
    session_id: str
    user_id: str
    start_time: datetime
    current_page: Optional[str] = None
    cart_items: list = None
    
    def __post_init__(self):
        if self.cart_items is None:
            self.cart_items = []
    
    @classmethod
    def create(cls, user_id: str) -> "UserSession":
        """Create a new user session."""
        return cls(
            session_id=str(uuid.uuid4()),
            user_id=user_id,
            start_time=datetime.utcnow(),
            cart_items=[]
        )