from sqlalchemy import create_engine, text

def create_tables(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cryptocurrencies (
                id TEXT PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                last_seen_at TIMESTAMP DEFAULT now()
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id SERIAL PRIMARY KEY,
                crypto_id TEXT REFERENCES cryptocurrencies(id),
                price_usd FLOAT,
                market_cap_usd FLOAT,
                volume_24h FLOAT,
                high_24h FLOAT,
                low_24h FLOAT,
                price_change_percentage_24h FLOAT,
                snapshot_ts TIMESTAMP DEFAULT now()
            );
        """))
        conn.commit()
        print("✅ Tablas creadas o verificadas.")