import pandas as pd
import numpy as np
import re

def process_catalog_clean(input_file, output_file):
    df = pd.read_csv(input_file)
    
    # 1. Filtre strict : Pas de Personal Care
    df = df[df['main_category'] != 'Personal Care'].copy()
    
    # 2. Nettoyage : On vire les lignes sans nom (il y en avait 2)
    df = df.dropna(subset=['display_name']).copy()

    # --- Logique de transformation (Brand, Sizes...) ---
    def extract_brand(display_name):
        if not isinstance(display_name, str): return str(display_name)
        keywords = ["Men", "Women", "Boys", "Girls", "Unisex"]
        pattern = r"\b(" + "|".join(keywords) + r")\b"
        match = re.search(pattern, display_name, re.IGNORECASE)
        if match:
            return display_name[:match.start()].strip()
        return display_name

    def assign_size_range(main_category):
        cat = str(main_category).lower()
        if 'apparel' in cat: return "XS,S,M,L,XL"
        elif 'footwear' in cat: return "36,37,38,39,40,41,42,43,44,45"
        else: return "Unique"

    # --- Construction ---
    df_out = pd.DataFrame()
    
    # Renérotation continue de 1 à N
    df_out['id_catalog_product'] = range(1, len(df) + 1)
    
    df_out['reference'] = df['reference']
    df_out['name'] = df['display_name']
    df_out['brand'] = df['display_name'].apply(extract_brand)
    df_out['color'] = df['color']
    df_out['season'] = df['season']
    df_out['sizes'] = df['main_category'].apply(assign_size_range)
    df_out['gender_segment'] = df['gender_segment']
    df_out['main_category'] = df['main_category']
    df_out['sub_category'] = df['sub_category']
    df_out['article_type'] = df['article_type']
    df_out['score_ec'] = 0
    
    np.random.seed(42)
    df_out['quantity'] = np.random.randint(500, 10000, size=len(df))

    df_out.to_csv(output_file, index=False)
    print(f"Fichier nettoyé généré : {len(df)} produits valides.")

process_catalog_clean('product-data-source.csv', 'ref_product_catalog_final.csv')