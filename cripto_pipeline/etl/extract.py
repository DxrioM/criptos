import requests

def get_top_cryptos(top_n=10):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": top_n,
        "page": 1
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()  # Lista de dicts con info de cada cripto
