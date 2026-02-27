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


import time
import requests

def get_route_osrm(origin_lat, origin_lon, dest_lat, dest_lon):
    url = (
        f"http://router.project-osrm.org/route/v1/driving/"
        f"{origin_lon},{origin_lat};{dest_lon},{dest_lat}"
        f"?overview=full&geometries=geojson"
    )

    last_err = None
    for attempt in range(1, 4):  # 3 essais
        try:
            r = requests.get(url, timeout=(5, 60))  # (connect timeout, read timeout)
            r.raise_for_status()
            data = r.json()

            coords = data["routes"][0]["geometry"]["coordinates"]  # [lon, lat]
            # on convertit en [(lat, lon)] pour publish MQTT
            return [(lat, lon) for lon, lat in coords]

        except Exception as e:
            last_err = e
            print(f"OSRM tentative {attempt}/3 échouée: {e}")
            time.sleep(1.5 * attempt)

    print("OSRM indisponible, fallback ligne droite.")
    return interpolate_straight_line(origin_lat, origin_lon, dest_lat, dest_lon, points=200)


def interpolate_straight_line(lat1, lon1, lat2, lon2, points=200):
    coords = []
    for i in range(points + 1):
        t = i / points
        lat = lat1 + (lat2 - lat1) * t
        lon = lon1 + (lon2 - lon1) * t
        coords.append((lat, lon))
    return coords


def interpolate_straight_line(lat1, lon1, lat2, lon2, points=200):
    coords = []
    for i in range(points + 1):
        t = i / points
        lat = lat1 + (lat2 - lat1) * t
        lon = lon1 + (lon2 - lon1) * t
        coords.append((lat, lon))
    return coords


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

    print("Simulation terminée")
    client.disconnect()

    print("Simulation terminée, passage en DELIVERED...")

    requests.patch(
        f"http://localhost:8087/api/delivery/{delivery_id}/status",
        params={"status": "DELIVERED"}
    )

    print("Statut forcé en DELIVERED")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python simulator.py <deliveryId>")
        sys.exit(1)

    delivery_id = int(sys.argv[1])
    main(delivery_id)