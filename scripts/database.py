# database.py
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String

def create_database_connection():
    """Crée et retourne la connexion à la base de données"""
    db_user = "postgres"
    db_password = "passer"
    db_host = "localhost"
    db_port = "5432"
    db_name = "meteo_db"
    
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
    return engine

def create_meteo_table(engine):
    """Crée la table meteo_tab si elle n'existe pas"""
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
    
    # Crée la table si elle n'existe pas
    metadata.create_all(engine)
    return meteo_tab