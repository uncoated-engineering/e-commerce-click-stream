"""
E-commerce data generator for creating realistic synthetic events.
"""
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import uuid

from faker import Faker


fake = Faker()


class EcommerceDataGenerator:
    """Generates synthetic e-commerce data."""
    
    def __init__(self, max_users: int = 1000, max_products: int = 500):
        self.max_users = max_users
        self.max_products = max_products
        
        # Product categories and their weights
        self.categories = {
            "Electronics": 0.25,
            "Clothing": 0.20,
            "Home & Garden": 0.15,
            "Books": 0.10,
            "Sports": 0.10,
            "Health & Beauty": 0.10,
            "Toys": 0.05,
            "Automotive": 0.05,
        }
        
        # Page types and their patterns
        self.page_patterns = {
            "homepage": "/",
            "category": "/category/{category}",
            "product": "/product/{product_id}",
            "cart": "/cart",
            "checkout": "/checkout",
            "search": "/search?q={query}",
        }
        
        # User agents for different devices
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15",
            "Mozilla/5.0 (Android 11; Mobile; rv:68.0) Gecko/68.0 Firefox/88.0",
        ]
        
        # Cache for generated data
        self._users = {}
        self._products = {}
        
    def generate_user_id(self) -> str:
        """Generate or return existing user ID."""
        if len(self._users) < self.max_users:
            user_id = str(uuid.uuid4())
            self._users[user_id] = {
                "created_at": fake.date_time_between(start_date="-1y", end_date="now"),
                "age": random.randint(18, 75),
                "country": fake.country_code(),
            }
            return user_id
        else:
            return random.choice(list(self._users.keys()))
    
    def generate_product_id(self) -> str:
        """Generate or return existing product ID."""
        if len(self._products) < self.max_products:
            product_id = str(uuid.uuid4())
            category = random.choices(
                list(self.categories.keys()), 
                weights=list(self.categories.values())
            )[0]
            
            self._products[product_id] = {
                "name": fake.catch_phrase(),
                "category": category,
                "price": round(random.uniform(5.99, 999.99), 2),
                "rating": round(random.uniform(1.0, 5.0), 1),
            }
            return product_id
        else:
            return random.choice(list(self._products.keys()))
    
    def generate_page_url(self, event_type: str, product_id: str = None) -> str:
        """Generate realistic page URL based on event type."""
        if event_type == "page_view":
            page_types = ["homepage", "category", "product", "search"]
            weights = [0.15, 0.25, 0.50, 0.10]
            page_type = random.choices(page_types, weights=weights)[0]
            
            if page_type == "homepage":
                return self.page_patterns["homepage"]
            elif page_type == "category":
                category = random.choice(list(self.categories.keys())).lower().replace(" & ", "-")
                return self.page_patterns["category"].format(category=category)
            elif page_type == "product":
                pid = product_id or self.generate_product_id()
                return self.page_patterns["product"].format(product_id=pid)
            elif page_type == "search":
                query = fake.word()
                return self.page_patterns["search"].format(query=query)
                
        elif event_type == "add_to_cart":
            pid = product_id or self.generate_product_id()
            return self.page_patterns["product"].format(product_id=pid)
            
        elif event_type == "purchase":
            return self.page_patterns["checkout"]
            
        return self.page_patterns["homepage"]
    
    def generate_user_agent(self) -> str:
        """Generate random user agent."""
        return random.choice(self.user_agents)
    
    def generate_ip_address(self) -> str:
        """Generate fake IP address."""
        return fake.ipv4()
    
    def get_event_type_probabilities(self, session_events: List[str]) -> Dict[str, float]:
        """
        Calculate event type probabilities based on user session history.
        This creates realistic user behavior patterns.
        """
        base_probs = {
            "page_view": 0.70,
            "add_to_cart": 0.20,
            "purchase": 0.10,
        }
        
        if not session_events:
            return base_probs
        
        # Adjust probabilities based on session history
        cart_adds = session_events.count("add_to_cart")
        page_views = session_events.count("page_view")
        
        # If user has items in cart, increase purchase probability
        if cart_adds > 0:
            base_probs["purchase"] = min(0.30, 0.10 + (cart_adds * 0.05))
            base_probs["add_to_cart"] = max(0.10, 0.20 - (cart_adds * 0.02))
            base_probs["page_view"] = 1.0 - base_probs["purchase"] - base_probs["add_to_cart"]
        
        # If many page views, increase cart add probability
        if page_views > 5:
            base_probs["add_to_cart"] = min(0.35, 0.20 + (page_views * 0.01))
            base_probs["page_view"] = max(0.40, 0.70 - (page_views * 0.01))
            base_probs["purchase"] = 1.0 - base_probs["add_to_cart"] - base_probs["page_view"]
        
        return base_probs
    
    def get_product_info(self, product_id: str) -> Dict:
        """Get product information by ID."""
        return self._products.get(product_id, {})
    
    def get_user_info(self, user_id: str) -> Dict:
        """Get user information by ID."""
        return self._users.get(user_id, {})