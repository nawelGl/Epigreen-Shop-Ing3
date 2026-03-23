import csv
import random
import uuid
from datetime import datetime
from locust import HttpUser, task, between

# --- 1. Data Loading ---
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
    
    # Base Host set to Event Tracker VM
    host = "http://172.31.250.47:4000"

    def on_start(self):
        self.token = ""
        self.user_id = ""
        
        if len(TEST_USERS) > 0:
            user = TEST_USERS.pop(0)
            # URL from CONFIG.API.CUSTOMER (assuming login is under this base)
            auth_url = "http://172.31.252.28:8081/api/auth/login"
            
            auth_payload = {
                "email": user["email"], 
                "id": user["password_hash"] 
            }
            
            try:
                res = self.client.post(auth_url, json=auth_payload, name="Login")
                if res.status_code == 200:
                    data = res.json()
                    self.token = data.get("token")
                    self.user_id = data.get("id")
                    print(f"AUTH: SUCCESS for {user.get('email')}")
                else:
                    print(f"AUTH: FAILED for {user.get('email')} - Status {res.status_code}")
            except Exception:
                print(f"AUTH: CONNECTION ERROR to 8081")

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

    @task(1)
    def add_to_cart(self):
        # Path from CONFIG.API.EVENTTRACKER
        url = "/api/track/events"
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        
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
        self.client.post(url, json=payload, headers=headers, name="Event_CART")