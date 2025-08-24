from etl.load import insert_qa
import smtplib
from email.mime.text import MIMEText

# Configuración básica de alertas
PRICE_ALERT_THRESHOLD = 10  # % de cambio en 24h para alertar
QA_ERROR_THRESHOLD = 5      # número de errores en QA para alertar

def check_price_alerts(engine, crypto):
    """
    Verifica si el cambio porcentual 24h supera el umbral.
    """
    change_pct = crypto.get("price_change_percentage_24h", 0)
    if abs(change_pct) >= PRICE_ALERT_THRESHOLD:
        message = f"⚠️ Alerta de precio: {crypto['name']} cambió {change_pct:.2f}% en 24h."
        print(message)
        # Opcional: enviar a QA también
        insert_qa(engine, crypto, "alerta_precio", message)

def check_qa_alerts(engine):
    """
    Revisa si hubo más de QA_ERROR_THRESHOLD errores en la última ejecución.
    """
    query = """
        SELECT COUNT(*) AS cnt FROM crypto_data_qa
        WHERE snapshot_ts::date = CURRENT_DATE;
    """
    with engine.connect() as conn:
        result = conn.execute(query)
        count = result.fetchone()[0]
        if count >= QA_ERROR_THRESHOLD:
            print(f"⚠️ Alerta QA: {count} registros fallaron hoy en la validación.")
