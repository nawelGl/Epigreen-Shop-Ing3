import requests
import random
import time

NB_WAREHOUSES = 5
OUTPUT_FILE = "insert_warehouses.sql"

def get_random_real_address():
    while True:
        lat = random.uniform(43.0, 50.0)
        lon = random.uniform(-1.0, 6.0)
        
        url = f"https://api-adresse.data.gouv.fr/reverse/?lat={lat}&lon={lon}&limit=1"
        
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                data = response.json()
                if data.get('features'):
                    props = data['features'][0]['properties']
                    if props.get('type') in ['housenumber', 'street', 'activity']:
                        return {
                            "street": props.get('name'),
                            "zip_code": props.get('postcode'),
                            "city": props.get('city'),
                            "gps_lat": data['features'][0]['geometry']['coordinates'][1],
                            "gps_long": data['features'][0]['geometry']['coordinates'][0]
                        }
            time.sleep(0.1)
        except:
            pass

print(f"🏭 Construction de {NB_WAREHOUSES} entrepôts logistiques...")

sql_lines = []
sql_lines.append("TRUNCATE TABLE warehouses CASCADE;")

types_entrepots = ["Plateforme Logistique", "Hub Central", "Centre de Distribution", "Entrepôt Régional", "Stockage"]

for i in range(NB_WAREHOUSES):
    addr = get_random_real_address()
    
    name = f"{random.choice(types_entrepots)} de {addr['city']}"
    
    name_clean = name.replace("'", "''")
    street_clean = addr['street'].replace("'", "''")
    city_clean = addr['city'].replace("'", "''")
    
    query = f"INSERT INTO warehouses (name, street, zip_code, city, country, gps_lat, gps_long) VALUES ('{name_clean}', '{street_clean}', '{addr['zip_code']}', '{city_clean}', 'France', {addr['gps_lat']}, {addr['gps_long']});"
    
    sql_lines.append(query)
    print(f"[{i+1}] {name}")

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(sql_lines))

print(f"\nFichier '{OUTPUT_FILE}' généré !")