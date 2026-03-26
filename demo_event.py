import csv
import random
import uuid
from datetime import datetime
from locust import HttpUser, task, between

# Data Load ==> données customers (id)
TEST_USERS = []
try:
    with open("customers.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            TEST_USERS.append(row)
    print(f"DEBUG: Loaded {len(TEST_USERS)} users")
except Exception as e:
    print(f"ERROR: Could not load CSV: {e}")

class EpigreenShopper(HttpUser):
    wait_time = between(1, 3)
    
    # Base Host Tracker VM
    host = "http://172.31.250.47:4000"

    def on_start(self):
        self.user_id = ""
        
        if len(TEST_USERS) > 0:
            user = TEST_USERS.pop(0)
            self.user_id = user["id"]
            print(f"USER ASSIGNED: {self.user_id}")

    @task(3)
    def click_product(self):
        # Path from CONFIG.API.EVENTTRACKER
        url = "/api/track/events"
        
        payload = {
            "eventId": str(uuid.uuid4()),
            "eventType": "CLICK",
            "userId": self.user_id,
            "eventData": {
                "productId": random.randint(1, 100)
            },
            "ts": datetime.utcnow().isoformat() + "Z",
            "metadata": { "device": "web" }
        }
        self.client.post(url, json=payload, name="Event_CLICK")


    @task(2)
    def search_product(self):
        url = "/api/track/events"
        search_keywords = ["T-shirt", "Nike", "Adidas", "Jean", "Skirt"]
        
        payload = {
            "eventId": str(uuid.uuid4()),
            "eventType": "SEARCH",
            "userId": self.user_id,
            "eventData": {
                "keyword": random.choice(search_keywords),
                "category": "All"
            },
            "ts": datetime.utcnow().isoformat() + "Z",
            "metadata": {"device": "web"}
        }
        self.client.post(url, json=payload, name="Event_SEARCH")

    @task(1)
    def add_to_cart(self):
        # Path from CONFIG.API.EVENTTRACKER
        url = "/api/track/events"
        
        payload = {
            "eventId": str(uuid.uuid4()),
            "eventType": "CART",
            "userId": self.user_id,
            "eventData": {
                "productId": random.randint(1, 100),
                "quantity": 1,
                "size": random.choice(["S", "M", "L", "XL"])
            },
            "ts": datetime.utcnow().isoformat() + "Z",
            "metadata": { "device": "web" }
        }
        self.client.post(url, json=payload, name="Event_CART")