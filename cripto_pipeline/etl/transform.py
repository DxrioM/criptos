import datetime
import re

# Columnas esperadas en la extracción
EXPECTED_COLUMNS = [
    "id", "name", "symbol", "current_price", "market_cap",
    "total_volume", "high_24h", "low_24h", "price_change_percentage_24h"
]

def normalize_name(name):
    """
    Normaliza nombres de criptomonedas:
    - Elimina espacios al inicio y final
    - Convierte a título (capitaliza cada palabra)
    - Remueve caracteres extraños
    """
    if not isinstance(name, str):
        return str(name)
    name = name.strip()
    name = re.sub(r"[^A-Za-z0-9\s]", "", name)
    name = name.title()
    return name

def validate_columns(crypto):
    """
    Valida que el dict solo contenga columnas esperadas
    """
    keys = set(crypto.keys())
    for key in keys:
        if key not in EXPECTED_COLUMNS:
            del crypto[key]  # elimina columnas extra
    # Asegura que todas las columnas esperadas existan
    for col in EXPECTED_COLUMNS:
        if col not in crypto:
            crypto[col] = None
    return crypto

def enforce_types(crypto):
    """
    Asegura que los tipos de datos sean consistentes
    """
    try:
        crypto["id"] = str(crypto["id"])
        crypto["name"] = str(crypto["name"])
        crypto["symbol"] = str(crypto["symbol"])
        crypto["current_price"] = float(crypto.get("current_price") or 0)
        crypto["market_cap"] = float(crypto.get("market_cap") or 0)
        crypto["total_volume"] = float(crypto.get("total_volume") or 0)
        crypto["high_24h"] = float(crypto.get("high_24h") or 0)
        crypto["low_24h"] = float(crypto.get("low_24h") or 0)
        crypto["price_change_percentage_24h"] = float(crypto.get("price_change_percentage_24h") or 0)
    except Exception as e:
        print(f"⚠️ Error en tipos de datos para {crypto.get('id')}: {e}")
    return crypto

def enrich_crypto_data(crypto, seen_ids=set(), engine=None):
    """
    Aplica todas las transformaciones:
    - Valida columnas
    - Normaliza nombres
    - Enforce types
    - Calcula métricas adicionales
    - Evita duplicados usando seen_ids
    - Registra datos inválidos en QA si se proporciona engine
    """
    crypto = validate_columns(crypto)

    # Normalizar nombre y símbolo
    crypto["name"] = normalize_name(crypto["name"])
    crypto["symbol"] = str(crypto["symbol"]).upper()

    # Validar duplicados
    if crypto["id"] in seen_ids:
        if engine:
            from etl.load import insert_qa
            insert_qa(engine, crypto, "duplicado", "Duplicado detectado en esta ejecución")
        return None
    seen_ids.add(crypto["id"])

    # Forzar tipos correctos
    try:
        crypto = enforce_types(crypto)
    except Exception as e:
        if engine:
            from etl.load import insert_qa
            insert_qa(engine, crypto, "tipo_incorrecto", str(e))
        return None

    # Métricas adicionales
    crypto["price_change_abs_24h"] = crypto["high_24h"] - crypto["low_24h"]
    crypto["price_vs_marketcap_ratio"] = (
        crypto["current_price"] / crypto["market_cap"] if crypto["market_cap"] > 0 else 0
    )
    crypto["snapshot_ts"] = datetime.datetime.now()

    return crypto
