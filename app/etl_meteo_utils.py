import requests
from pandas import json_normalize
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String
import os
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")

# ----------------------------
# CONFIG
# ----------------------------

DB_CONN = os.getenv("DB_CONN")
API_KEY = os.getenv("API_KEY")
LAT = os.getenv("LAT")
LON = os.getenv("LON")

engine = create_engine(DB_CONN)


# ------------------------------------
#  EXTRACT : Récupérer les données
# ------------------------------------
def extract_weather(**context):
    url = (
        f"https://api.openweathermap.org/data/3.0/onecall?"
        f"lat={LAT}&lon={LON}&exclude=hourly,daily&appid={API_KEY}"
    )

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Erreur API : {response.status_code} - {response.text}")

    data = response.json()
    context["ti"].xcom_push(key="raw_weather", value=data)


# ------------------------------------
#  TRANSFORM : nettoyer et transformer
# ------------------------------------
def transform_weather(**context):
    data = context["ti"].xcom_pull(key="raw_weather", task_ids="extract_weather")

    df = json_normalize(data)
    df = df.rename(columns={col: col.replace("current.", "") for col in df.columns})

    # Extraire les infos du tableau weather
    df["weather_id"] = df["weather"].apply(lambda x: x[0]["id"] if x else None)
    df["weather_icon"] = df["weather"].apply(lambda x: x[0]["icon"] if x else None)
    df["weather_main"] = df["weather"].apply(lambda x: x[0]["main"] if x else None)
    df["weather_description"] = df["weather"].apply(lambda x: x[0]["description"] if x else None)

    df = df.rename(columns={"lat": "latitude", "lon": "longitude"})
    df = df.drop(columns=["timezone_offset", "dt", "sunrise", "sunset", "weather"])

    context["ti"].xcom_push(key="clean_df", value=df.to_dict(orient="records"))


# ------------------------------------
# LOAD : charger dans PostgreSQL
# ------------------------------------
def load_weather(**context):
    records = context["ti"].xcom_pull(key="clean_df", task_ids="transform_weather")
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
        Column("weather_description", String(100)),
    )

    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(meteo_tab.insert(), records)

    print("✔ Données insérées dans PostgreSQL avec succès.")
