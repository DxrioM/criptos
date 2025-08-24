from sqlalchemy import text
import json
import logging

def upsert_crypto(engine, crypto):
    query = """
    INSERT INTO cryptocurrencies (id, name, symbol, last_seen_at)
    VALUES (:id, :name, :symbol, now())
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        symbol = EXCLUDED.symbol,
        last_seen_at = now();
    """
    with engine.connect() as conn:
        conn.execute(text(query), {
            "id": crypto["id"],
            "name": crypto["name"],
            "symbol": crypto["symbol"]
        })
        conn.commit()

def insert_price(engine, crypto):
    query = """
    INSERT INTO crypto_prices (
        crypto_id, price_usd, market_cap_usd, volume_24h,
        high_24h, low_24h, price_change_percentage_24h, snapshot_ts
    ) VALUES (
        :crypto_id, :price_usd, :market_cap_usd, :volume_24h,
        :high_24h, :low_24h, :price_change_percentage_24h, now()
    );
    """
    with engine.connect() as conn:
        conn.execute(text(query), {
            "crypto_id": crypto["id"],
            "price_usd": crypto["current_price"],
            "market_cap_usd": crypto.get("market_cap", 0),
            "volume_24h": crypto.get("total_volume", 0),
            "high_24h": crypto.get("high_24h", 0),
            "low_24h": crypto.get("low_24h", 0),
            "price_change_percentage_24h": crypto.get("price_change_percentage_24h", 0)
        })
        conn.commit()

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def insert_qa(engine, crypto, error_type, error_details=""):
    """
    Inserta un registro de QA en la tabla crypto_data_qa.
    
    Params:
        engine: SQLAlchemy engine
        crypto (dict | None): Datos crudos de la criptomoneda.
        error_type (str): Tipo de error o estado ("MISSING_FIELD", "API_ERROR", "SUCCESS", etc.)
        error_details (str): Descripción detallada del error o validación.
    """

    # Validar el objeto crypto
    if crypto is not None and not isinstance(crypto, dict):
        logger.error("El parámetro 'crypto' debe ser un dict o None.")
        crypto = None
        error_type = "INVALID_FORMAT"
        error_details = "crypto no es un diccionario válido"

    # Extraer ID si existe
    crypto_id = crypto.get("id") if crypto and "id" in crypto else None
    if not crypto_id and error_type == "SUCCESS":
        # Caso donde esperábamos un id pero no existe
        error_type = "MISSING_FIELD"
        error_details = "No se encontró el campo 'id' en los datos crudos"

    # Convertir a JSON seguro
    try:
        raw_data_json = json.dumps(crypto, ensure_ascii=False) if crypto else "{}"
    except Exception as e:
        logger.exception("Error serializando crypto a JSON")
        raw_data_json = "{}"
        error_type = "JSON_ERROR"
        error_details = str(e)

    query = """
    INSERT INTO crypto_data_qa (crypto_id, error_type, error_details, raw_data, snapshot_ts)
    VALUES (:crypto_id, :error_type, :error_details, :raw_data::jsonb, now());
    """

    try:
        with engine.begin() as conn:  # begin() maneja el commit/rollback automáticamente
            conn.execute(
                text(query),
                {
                    "crypto_id": crypto_id,
                    "error_type": error_type,
                    "error_details": error_details,
                    "raw_data": raw_data_json,
                },
            )
        logger.info(f"QA registrado: crypto_id={crypto_id}, error_type={error_type}")
    except Exception as e:
        logger.exception("Error insertando en crypto_data_qa")

