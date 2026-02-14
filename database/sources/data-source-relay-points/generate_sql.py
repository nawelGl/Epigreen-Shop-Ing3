import requests
import random
import time
from faker import Faker

NB_POINTS = 200
OUTPUT_FILE = "insert_relays.sql"
fake = Faker('fr_FR')

def get_random_real_address():
    while True:
        # Coordonnées approx de la France
        lat = random.uniform(42.5, 51.0)
        lon = random.uniform(-4.5, 8.0)
        
        url = f"https://api-adresse.data.gouv.fr/reverse/?lat={lat}&lon={lon}&limit=1"
        
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                data = response.json()
                if data.get('features'):
                    props = data['features'][0]['properties']
                    if props.get('type') in ['housenumber', 'street']:
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

print(f"Fabrication du fichier SQL avec {NB_POINTS} vraies adresses...")

sql_lines = []

sql_lines.append("TRUNCATE TABLE relay_points CASCADE;")

for i in range(NB_POINTS):
    addr = get_random_real_address()
    
    shop_types = ["Pressing", "Tabac", "Relais", "Vival", "Boulangerie", "Fleuriste"]
    shop_name = f"{random.choice(shop_types)} {fake.last_name()}"
    
    name_clean = shop_name.replace("'", "''")
    street_clean = addr['street'].replace("'", "''")
    city_clean = addr['city'].replace("'", "''")
    
    query = f"INSERT INTO relay_points (name, street, zip_code, city, country, gps_lat, gps_long) VALUES ('{name_clean}', '{street_clean}', '{addr['zip_code']}', '{city_clean}', 'France', {addr['gps_lat']}, {addr['gps_long']});"
    
    sql_lines.append(query)
    print(f"[{i+1}] {shop_name} - {addr['zip_code']}")

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(sql_lines))

print(f"\n✨ Terminé ! Le fichier '{OUTPUT_FILE}' est enregistré.")