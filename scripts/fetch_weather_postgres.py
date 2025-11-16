import requests
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String


from sqlalchemy import create_engine, text

# Créer la chaîne de connexion PostgreSQL
db_user = "postgres"
db_password = "passer"
db_host = "localhost"
db_port = "5432"
db_name = "meteo_db"

# Connexion via SQLAlchemy
engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# Paramètres
lat="48.858370"
lon="2.294481"
api_key = "5a81b86257c58da964a779f348591c7a"
# URL de l'API
url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude=hourly,daily&appid={api_key}"

# Requête vers l'API
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print("Données récupérées avec succès !")
else:
    raise Exception(f"Erreur {response.status_code} : {response.text}")

# Aplatir le JSON
df = json_normalize(data)
#print(df.columns)

df = df.rename(columns={col: col.replace('current.', '') for col in df.columns})

df["weather_id"] = df["weather"].apply(lambda x: x[0]["id"] if isinstance(x, list) and len(x) > 0 else None)
df["weather_icon"] = df["weather"].apply(lambda x: x[0]["icon"] if isinstance(x, list) and len(x) > 0 else None)
df["weather_main"] = df["weather"].apply(lambda x: x[0]["main"] if isinstance(x, list) and len(x) > 0 else None)
df["weather_description"] = df["weather"].apply(lambda x: x[0]["description"] if isinstance(x, list) and len(x) > 0 else None)

df = df.rename(columns={
    "lat": "latitude",
    "lon": "longitude"
})
df = df.drop(columns=['timezone_offset', 'dt', 'sunrise', 'sunset', 'weather'])

# --- Define SQLAlchemy Table ---
metadata = MetaData()

meteo_tab = Table(
    "meteo_tab",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timezone", String(50)),
    Column("temp", Float),
    Column("feels_like", Float),
    Column("pressure", Float),
    Column("humidity", Float),
    Column("dew_point", Float),
    Column("uvi", Float),
    Column("clouds", Float), 
    Column("visibility", Float),
    Column("wind_speed", Float),
    Column("wind_deg", Float),
    Column("weather_id", Integer),
    Column("weather_icon", String(10)),
    Column("weather_main", String(50)),
    Column("weather_description", String(100))
)

# Create the table in MySQL if it doesn’t exist
metadata.create_all(engine)

# --- Insert DataFrame using SQLAlchemy Core ---
# Convert DataFrame to a list of dictionaries (records)
records = df.to_dict(orient="records")

with engine.begin() as conn:
    conn.execute(meteo_tab.insert(), records)

print("Data successfully inserted into PostgreSQL using SQLAlchemy Core!")
