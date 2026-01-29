import json, time, uuid, random
import pandas as pd
from datetime import datetime, timezone
from confluent_kafka import Producer
import pandas as pd
import os
import kagglehub


BOOTSTRAP = ADDRESSE_KAFKA

TOPIC_CLICK  = "user-event-click"
TOPIC_CART   = "user-event-cart"
TOPIC_SEARCH = "user-event-search"

# Download latest version
path = kagglehub.dataset_download("retailrocket/ecommerce-dataset")

print("Path to dataset files:", path)

# récupération du fichier csv depuis kagglehub
df=pd.read_csv(os.path.join(path,"events.csv"))
df.head()

RATE_PER_SEC = 1
SEARCH_PROB = 0.2  # la probabilité de génération de recherce


#Ceclui qui envoie les messages à kafka avec Produce()
# bootstrap.servers = adresse de kafka, linger.ms = on rammase envrion 20ms et on batch
p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20})

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def send(topic: str, key: str, payload: dict):
    p.produce(topic, key=key.encode("utf-8"), value=json.dumps(payload, ensure_ascii=False).encode("utf-8"))

# sent : counter de l'envoi
sent = 0
for r in df.itertuples(index=False):
    user_id = str(r.visitorid)
    item_id = str(r.itemid)
    ev = str(r.event).lower()  # click/cart/purchase

    # le dataset trouvé n'a pas d'information sur la recherche, on insère quelques evènement search avant click
    if SEARCH_PROB > 0 and ev == "view" and random.random() < SEARCH_PROB:
        send(TOPIC_SEARCH, user_id, {
            "event_id": str(uuid.uuid4()),
            "event_type": "search",
            "user_id": user_id,
            "ts": now_iso(),
            "metadata": {"query": f"search_item_{int(r.itemid)%1000}", "device": "simulated"}
        })
        sent += 1

    payload = {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "product_id": item_id,
        "ts": now_iso(),
        "metadata": {"device": "simulated"}
    }
    
    # we check what kind of event for each line and send it to proper kafka topic

    if ev == "view":
        payload["event_type"] = "click"
        send(TOPIC_CLICK, user_id, payload)

    elif ev == "addtocart":
        payload["event_type"] = "cart"
        payload["metadata"].update({"action": "add", "quantity": 1})
        send(TOPIC_CART, user_id, payload)

    elif ev == "transaction":
        continue

    else:
        payload["event_type"] = "click"
        send(TOPIC_CLICK, user_id, payload)

    sent += 1

    if sent % 10000 == 0:
        p.flush()
    if RATE_PER_SEC and RATE_PER_SEC > 0:
        time.sleep(1.0 / RATE_PER_SEC)

p.flush()
print("=====script bien terminé====== nombre d'évènement envoyé: ", sent)