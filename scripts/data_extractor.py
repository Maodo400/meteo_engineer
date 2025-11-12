# data_extractor.py
import requests
import pandas as pd
from pandas import json_normalize

def fetch_weather_data(lat="48.858370", lon="2.294481"):
    """Récupère les données météo depuis l'API OpenWeather"""
    api_key = "5a81b86257c58da964a779f348591c7a"
    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude=hourly,daily&appid={api_key}"

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        print("Données récupérées avec succès !")
        return data
    else:
        raise Exception(f"Erreur {response.status_code} : {response.text}")

def transform_weather_data(data):
    """Transforme et nettoie les données météo"""
    # Aplatir le JSON
    df = json_normalize(data)
    
    # Renommer les colonnes
    df = df.rename(columns={col: col.replace('current.', '') for col in df.columns})
    
    # Extraire les données weather
    df["weather_id"] = df["weather"].apply(lambda x: x[0]["id"] if isinstance(x, list) and len(x) > 0 else None)
    df["weather_icon"] = df["weather"].apply(lambda x: x[0]["icon"] if isinstance(x, list) and len(x) > 0 else None)
    df["weather_main"] = df["weather"].apply(lambda x: x[0]["main"] if isinstance(x, list) and len(x) > 0 else None)
    df["weather_description"] = df["weather"].apply(lambda x: x[0]["description"] if isinstance(x, list) and len(x) > 0 else None)
    
    # Renommer les colonnes de coordonnées
    df = df.rename(columns={
        "lat": "latitude",
        "lon": "longitude"
    })
    
    # Supprimer les colonnes inutiles
    df = df.drop(columns=['timezone_offset', 'dt', 'sunrise', 'sunset', 'weather'])
    
    return df