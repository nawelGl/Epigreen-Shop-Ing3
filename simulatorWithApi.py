import time
import json
import requests
import sys
import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
TOPIC = "delivery/location"

DELIVERY_API = "http://localhost:8087/api/delivery"

STEP_EVERY_N_POINTS = 5
SLEEP_SECONDS = 1


def get_delivery(delivery_id):
    url = f"{DELIVERY_API}/{delivery_id}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()


def get_route_osrm(origin_lat, origin_lon, dest_lat, dest_lon):
    url = (
        f"http://router.project-osrm.org/route/v1/driving/"
        f"{origin_lon},{origin_lat};{dest_lon},{dest_lat}"
        f"?overview=full&geometries=geojson"
    )
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    return data["routes"][0]["geometry"]["coordinates"]


def main(delivery_id):
    print(f"🔎 Récupération livraison {delivery_id}...")

    delivery = get_delivery(delivery_id)

    origin_lat = delivery["originLat"]
    origin_lon = delivery["originLon"]
    dest_lat = delivery["destLat"]
    dest_lon = delivery["destLon"]

    print("📍 Origine:", origin_lat, origin_lon)
    print("📍 Destination:", dest_lat, dest_lon)

    print("🛣️ Calcul route via OSRM...")
    coords = get_route_osrm(origin_lat, origin_lon, dest_lat, dest_lon)

    print(f"Route récupérée: {len(coords)} points")

    client = mqtt.Client()
    client.connect(BROKER, PORT, 60)

    for i in range(0, len(coords), STEP_EVERY_N_POINTS):
        lon, lat = coords[i]

        payload = {
            "deliveryId": delivery_id,
            "lat": lat,
            "lon": lon
        }

        client.publish(TOPIC, json.dumps(payload))
        print("Published:", payload)

        time.sleep(SLEEP_SECONDS)

    print("✅ Simulation terminée")
    client.disconnect()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python simulator.py <deliveryId>")
        sys.exit(1)

    delivery_id = int(sys.argv[1])
    main(delivery_id)