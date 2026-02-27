import paho.mqtt.client as mqtt
import time
import json

# Configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "delivery/location"

# Trajet enrichi (Entrepôt Sud de Paris -> Centre Paris)
path = [
    (48.7500, 2.3000), (48.7550, 2.3050), (48.7600, 2.3100), (48.7650, 2.3150),
    (48.7700, 2.3200), (48.7800, 2.3250), (48.7900, 2.3300), (48.8000, 2.3350),
    (48.8100, 2.3380), (48.8200, 2.3400), (48.8300, 2.3420), (48.8350, 2.3440),
    (48.8400, 2.3460), (48.8450, 2.3480), (48.8500, 2.3500), (48.8520, 2.3510),
    (48.8540, 2.3515), (48.8555, 2.3520), (48.8560, 2.3521), (48.8566, 2.3522)
]

client = mqtt.Client()
client.connect(MQTT_BROKER, MQTT_PORT, 60)

def run_simulation(delivery_id):
    print(f"📡 Début de la simulation longue pour la livraison #{delivery_id}")
    for i, (lat, lon) in enumerate(path):
        raw_data = json.dumps({
            "deliveryId": delivery_id,
            "lat": lat,
            "lon": lon,
            "timestamp": time.time()
        })
        
        client.publish(MQTT_TOPIC, raw_data)
        print(f"[{i+1}/{len(path)}] 📤 Position envoyée : {lat}, {lon}")
        
        # On attend 3 secondes pour laisser le temps au Front de rafraîchir
        time.sleep(3) 
    
    print("✅ Simulation terminée !")

if __name__ == "__main__":
    # N'oublie pas de mettre l'ID de ta livraison ici !
    run_simulation(delivery_id=10)