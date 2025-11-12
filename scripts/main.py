# main.py
from database import create_database_connection, create_meteo_table
from data_extractor import fetch_weather_data, transform_weather_data

def main():
    """Fonction principale qui orchestre le processus ETL"""
    try:
        # E - Extraction
        print("Étape 1: Extraction des données...")
        weather_data = fetch_weather_data()
        
        # T - Transformation
        print("Étape 2: Transformation des données...")
        df = transform_weather_data(weather_data)
        
        # L - Chargement
        print("Étape 3: Chargement dans la base de données...")
        
        # Connexion à la base
        engine = create_database_connection()
        
        # Création de la table
        meteo_table = create_meteo_table(engine)
        
        # Insertion des données
        records = df.to_dict(orient="records")
        
        with engine.begin() as conn:
            conn.execute(meteo_table.insert(), records)
        
        print("Données insérées avec succès dans PostgreSQL !")
        print(f"{len(records)} enregistrement(s) ajouté(s)")
        
    except Exception as e:
        print(f"❌Erreur: {e}")

if __name__ == "__main__":
    main()