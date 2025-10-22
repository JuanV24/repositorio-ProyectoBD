
from sqlalchemy import create_engine, text
import urllib.parse
import pandas as pd

#CONEXION A BASES DE DATOS
# --- Credenciales comunes ---
username = "postgres"
password = urllib.parse.quote_plus("VELASQUEZ74")  # Protege por si hay símbolos
host = "localhost"
port = "5432"

# --- Bases de datos ---
db_src = "chinook"
db_dw = "dwchinook"

# --- Cadenas de conexión ---
url_src = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_src}"
url_dw = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_dw}"

# --- Creación de engines ---
engine_src = create_engine(url_src)
engine_dw = create_engine(url_dw)

# --- Pruebas de conexión ---
try:
    with engine_src.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        print("Conexión exitosa a base de datos ORIGEN (Chinook):")
        for row in result:
            print(row[0])

    with engine_dw.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        print("\n Conexión exitosa a base de datos DESTINO (DW_Chinook):")
        for row in result:
            print(row[0])

except Exception as e:
    print("Error de conexión:")
    print(e)


