import yfinance as yf
import psycopg2
import time
from datetime import datetime
import requests
from newsapi import NewsApiClient

# --- CONFIGURATION ---
DB_NAME = "fin_data_db"
DB_USER = "postgres"
DB_PASS = "Piya082001@"  # <<< PASTE YOUR PASSWORD HERE
DB_HOST = "localhost"
DB_PORT = "5432"

NEWS_API_KEY = "ca7f210ad2ec4a6b9186dc9fe84f67a2" # <<< PASTE YOUR NEWS API KEY HERE

NSE_TICKERS = ['RELIANCE.NS', 'TCS.NS']
CRYPTO_PAIRS = ['btcinr', 'ethinr']
NEWS_SEARCH_TERMS = ['Reliance Industries', 'Bitcoin India']
WAZIRX_API_URL = "https://api.wazirx.com/api/v2/tickers"
COINDCX_API_URL = "https://api.coindcx.com/exchange/ticker"


def fetch_and_store_nse_data(db_connection):
    """Fetches stock data and stores it in the database."""
    cursor = db_connection.cursor()
    print("  > Fetching NSE stock data...")
    for ticker in NSE_TICKERS:
        try:
            stock_data = yf.Ticker(ticker).history(period="1d")
            if not stock_data.empty:
                latest_data = stock_data.iloc[-1]
                data_to_insert = ('NSE_yfinance', ticker, float(latest_data['Close']), int(latest_data['Volume']), latest_data.name)
                insert_query = "INSERT INTO price_data (source, asset_ticker, price, volume, timestamp) VALUES (%s, %s, %s, %s, %s);"
                cursor.execute(insert_query, data_to_insert)
                print(f"    - Stored {ticker} @ {latest_data['Close']:.2f}")
        except Exception as e:
            print(f"    - Failed for {ticker}: {e}")
    db_connection.commit()
    cursor.close()

def fetch_and_store_wazirx_data(db_connection):
    """Fetches crypto data from WazirX API and stores it."""
    cursor = db_connection.cursor()
    print("  > Fetching WazirX crypto data...")
    try:
        response = requests.get(WAZIRX_API_URL)
        response.raise_for_status()
        all_tickers = response.json()
        for pair in CRYPTO_PAIRS:
            if pair in all_tickers:
                ticker_data = all_tickers[pair]
                timestamp = datetime.fromtimestamp(int(ticker_data['at']))
                data_to_insert = ('WazirX_API', pair.upper(), float(ticker_data['last']), float(ticker_data['volume']), timestamp)
                insert_query = "INSERT INTO price_data (source, asset_ticker, price, volume, timestamp) VALUES (%s, %s, %s, %s, %s);"
                cursor.execute(insert_query, data_to_insert)
                print(f"    - Stored {pair.upper()} (WazirX) @ {float(ticker_data['last']):.2f}")
    except Exception as e:
        print(f"    - Failed to fetch WazirX data: {e}")
    db_connection.commit()
    cursor.close()

def fetch_and_store_coindcx_data(db_connection):
    """Fetches crypto data from CoinDCX API and stores it."""
    cursor = db_connection.cursor()
    print("  > Fetching CoinDCX crypto data...")
    try:
        response = requests.get(COINDCX_API_URL)
        response.raise_for_status()
        all_tickers = response.json()
        for ticker_data in all_tickers:
            pair_lower = ticker_data['market'].lower()
            if pair_lower in CRYPTO_PAIRS:
                timestamp = datetime.fromtimestamp(ticker_data['timestamp'] / 1000)
                data_to_insert = ('CoinDCX_API', pair_lower.upper(), float(ticker_data['last_price']), float(ticker_data['volume']), timestamp)
                insert_query = "INSERT INTO price_data (source, asset_ticker, price, volume, timestamp) VALUES (%s, %s, %s, %s, %s);"
                cursor.execute(insert_query, data_to_insert)
                print(f"    - Stored {pair_lower.upper()} (CoinDCX) @ {float(ticker_data['last_price']):.2f}")
    except Exception as e:
        print(f"    - Failed to fetch CoinDCX data: {e}")
    db_connection.commit()
    cursor.close()

def fetch_and_store_news_data(db_connection):
    """Fetches news headlines and stores them, avoiding duplicates."""
    print("  > Fetching market news...")
    newsapi = NewsApiClient(api_key=NEWS_API_KEY)
    cursor = db_connection.cursor()
    for term in NEWS_SEARCH_TERMS:
        try:
            all_articles = newsapi.get_everything(q=term, language='en', sort_by='publishedAt', page_size=10)
            
            insert_count = 0
            for article in all_articles['articles']:
                # --- THIS IS THE IMPROVEMENT ---
                # First, check if the headline already exists
                check_query = "SELECT id FROM market_news WHERE headline = %s;"
                cursor.execute(check_query, (article['title'],))
                exists = cursor.fetchone()

                # Only insert if the headline does not exist
                if not exists:
                    insert_query = "INSERT INTO market_news (asset_ticker, source_name, headline, published_at) VALUES (%s, %s, %s, %s);"
                    data_to_insert = (term, article['source']['name'], article['title'], article['publishedAt'])
                    cursor.execute(insert_query, data_to_insert)
                    insert_count += 1
            
            print(f"    - Stored {insert_count} new headlines for {term}")

        except Exception as e:
            print(f"    - Failed to fetch news for {term}: {e}")
    db_connection.commit()
    cursor.close()

# --- Main Loop ---
if __name__ == "__main__":
    while True:
        try:
            print(f"\n[{datetime.now()}] Starting data harvesting cycle...")
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
            
            # We pass the connection object directly to each function
            fetch_and_store_nse_data(conn)
            fetch_and_store_wazirx_data(conn)
            fetch_and_store_coindcx_data(conn)
            fetch_and_store_news_data(conn)
            
            conn.close()
            print("  > Cycle complete. Connection closed.")
            print("  > Waiting for 15 minutes...")
            time.sleep(900) # 15 minutes
        except Exception as e:
            print(f"An error occurred in the main harvester loop: {e}")
            time.sleep(900)