import websocket
import threading
import time
import requests
import random

# --- CONFIGURATION ---
KONG_WS_URL = "ws://localhost:8000/notifications"
DELIVERY_API_URL = "http://localhost:8087/api/delivery" # Vérifie bien le port 8087 !
USERS_TO_SIMULATE = range(101, 151) # 50 utilisateurs de 101 à 150

# ==========================================
# 1. PARTIE WEBSOCKET (Les clients qui écoutent)
# ==========================================
def on_message(ws, message):
    print(f"\n [FRONT-END USER {ws.user_id}] NOTIF REÇUE : {message}\n")

def on_error(ws, error):
    pass # On masque les petites erreurs de réseau pour une démo propre

def on_close(ws, close_status_code, close_msg):
    pass

def on_open(ws):
    pass # On masque le log d'ouverture pour ne pas polluer l'écran au lancement des 50 clients

def connect_client(user_id):
    url = f"{KONG_WS_URL}?userId={user_id}"
    while True:
        ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
        ws.user_id = user_id
        ws.run_forever(ping_interval=30, ping_timeout=10)

        time.sleep(2)

# ==========================================
# 2. PARTIE API (Le système qui crée la donnée)
# ==========================================
def trigger_backend_event(user_id):
    print(f"\n [BACK-END] Création d'une commande fantôme pour l'User {user_id}...")
    
    # 1. Création de la livraison (En utilisant le format de ton Checkout.jsx)
    delivery_data = {
        "orderId": random.randint(100000, 9999999), # Faux ID de commande pour éviter les conflits
        "customerId": user_id,
        "originWarehouseId": 1,
        "originLat": 48.75, "originLon": 2.30,
        "destStreet": "10 Rue de la Démo", "destCity": "Paris", "destZipCode": "75000",
        "destLat": 48.86, "destLon": 2.33,
        "deliveryMethod": "DOMICILE"
    }
    
    try:
        # Appelle ms-delivery pour créer
        res = requests.post(f"{DELIVERY_API_URL}/create", json=delivery_data)
        delivery_id = res.json().get("id")
        
        if not delivery_id:
            print(f" Échec de la création pour l'User {user_id}")
            return
            
        print(f"[BACK-END] Livraison {delivery_id} créée. Acheminement en cours (attente 2s)...")
        time.sleep(2)
        
        # 2. Déclenchement de la notification via le changement de statut !
        print(f"[BACK-END] Passage de la livraison {delivery_id} en DELIVERED !")
        requests.patch(f"{DELIVERY_API_URL}/{delivery_id}/status", params={"status": "DELIVERED"})
        print("[BACK-END] L'événement Kafka a été propulsé !")
        
    except Exception as e:
        print(f"Erreur de connexion à l'API ms-delivery : {e}")

# ==========================================
# 3. LE CHEF D'ORCHESTRE (Menu interactif)
# ==========================================
if __name__ == "__main__":
    print("DÉMARRAGE DE LA DÉMO CLUSTER EPIGREEN")
    print("-----------------------------------------")
    print(f"Connexion de {len(USERS_TO_SIMULATE)} utilisateurs via le Load Balancer Kong...")
    
    for i in USERS_TO_SIMULATE:
        t = threading.Thread(target=connect_client, args=(i,))
        t.daemon = True
        t.start()
        time.sleep(0.05) # Petit délai pour laisser le temps à Kong de répartir
        
    print("Tous les clients sont en écoute silencieuse.\n")
    
    try:
        while True:
            # Le script se met en pause et attend ton signal pour la démo
            input("Appuie sur [ENTRÉE] pour tirer une notification aléatoire (ou Ctrl+C pour quitter)...")
            
            # On choisit un utilisateur au hasard parmi les 50
            random_user = random.choice(USERS_TO_SIMULATE)
            
            # On déclenche la chaîne back-end dans un Thread pour ne pas bloquer le script
            t_event = threading.Thread(target=trigger_backend_event, args=(random_user,))
            t_event.start()
            
    except KeyboardInterrupt:
        print("\n Fin de la démonstration.")