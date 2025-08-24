import os
import time
import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

from db.setup_tables import create_tables
from etl.extract import get_top_cryptos
from etl.load import upsert_crypto, insert_price
from etl.transform import enrich_crypto_data

# ===== 1. Cargar variables del .env =====
load_dotenv()
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")
PIPELINE_INTERVAL_MINUTES = int(os.getenv("PIPELINE_INTERVAL_MINUTES", 30))
TOP_N_CRYPTOS = int(os.getenv("TOP_N_CRYPTOS", 10))

# ===== 2. Conexión a PostgreSQL =====
engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)

# ===== 3. Crear tablas si no existen =====
create_tables(engine)

# ===== 4. Loop principal del pipeline =====
seen_ids = set()

def run_pipeline():
    cryptos = get_top_cryptos(TOP_N_CRYPTOS)
    snapshot_ts = datetime.datetime.now()
    print(f"🔹 Snapshot a las {snapshot_ts}")

    for crypto in cryptos:
        enriched_crypto = enrich_crypto_data(crypto, seen_ids, engine)
        if enriched_crypto is None:
            continue  # los errores ya fueron guardados en QA

        upsert_crypto(engine, enriched_crypto)
        insert_price(engine, enriched_crypto)

        print(f"💰 {enriched_crypto['name']} (${enriched_crypto['current_price']}) guardado.")


if __name__ == "__main__":
    while True:
        run_pipeline()
        print(f"⏱ Esperando {PIPELINE_INTERVAL_MINUTES} minutos...\n")
        time.sleep(PIPELINE_INTERVAL_MINUTES * 60)
