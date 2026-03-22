import csv
import random
from locust import HttpUser, task, between

# --- 1. Load Data ---
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
    host = "http://172.31.250.47:3000"

    def on_start(self):
        self.token = ""
        if len(TEST_USERS) > 0:
            user = TEST_USERS.pop(0)
            auth_url = "http://172.31.252.28:8081/api/auth/login"
            
            print(f"AUTH: Login start for {user.get('email')}")
            try:
                payload = {"email": user["email"], "password": user["password_hash"]}
                res = self.client.post(auth_url, json=payload)
                if res.status_code == 200:
                    self.token = res.json().get("token")
                    print(f"AUTH: Success for {user.get('email')}")
                else:
                    print(f"AUTH: Failed for {user.get('email')} - Status {res.status_code}")
            except Exception as e:
                print(f"AUTH: Error connecting to auth server")

    @task(3)
    def click_product(self):
        url = "http://172.31.250.47:3000/api/events"
        data = {"type": "CLICK", "productId": random.randint(1, 100)}
        self.client.post(url, json=data, name="CLICK")

    @task(1)
    def add_to_cart(self):
        url = "http://172.31.250.47:3000/api/events"
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        data = {
            "type": "CART",
            "productId": random.randint(1, 100),
            "quantity": 1
        }
        self.client.post(url, json=data, headers=headers, name="CART")