import psycopg2
import time
from datetime import datetime, date
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pandas as pd
from prophet import Prophet
import yfinance as yf

# --- CONFIGURATION ---
DB_NAME = "fin_data_db"
DB_USER = "postgres"
DB_PASS = "Piya082001@"  # <<< PASTE YOUR PASSWORD HERE
DB_HOST = "localhost"
DB_PORT = "5432"

CRYPTO_PAIRS_TO_ANALYZE = ['BTCINR', 'ETHINR']
STOCKS_TO_PREDICT = ['RELIANCE.NS', 'TCS.NS']
ARB_THRESHOLD_PERCENT = 0.05


def find_arbitrage_opportunities(db_connection):
    """Analyzes the latest price data to find and store arbitrage opportunities."""
    print("  > Analyzing for arbitrage opportunities...")
    cursor = db_connection.cursor()
    for pair in CRYPTO_PAIRS_TO_ANALYZE:
        try:
            query = """
                SELECT source, price FROM price_data
                WHERE asset_ticker = %s AND timestamp > NOW() - INTERVAL '5 minutes'
                ORDER BY timestamp DESC;
            """
            cursor.execute(query, (pair,))
            rows = cursor.fetchall()
            latest_prices = {}
            for row in rows:
                source, price = row[0], row[1]
                if source not in latest_prices:
                    latest_prices[source] = price
            
            if len(latest_prices) < 2:
                continue

            buy_source = min(latest_prices, key=latest_prices.get)
            buy_price = latest_prices[buy_source]
            sell_source = max(latest_prices, key=latest_prices.get)
            sell_price = latest_prices[sell_source]

            if buy_price > 0:
                profit_percent = ((sell_price - buy_price) / buy_price) * 100
            else:
                profit_percent = 0

            if profit_percent > ARB_THRESHOLD_PERCENT:
                details = (f"Buy {pair} on {buy_source} at {buy_price:.2f} and "
                           f"Sell on {sell_source} at {sell_price:.2f}. "
                           f"Potential Profit: {profit_percent:.2f}%")
                print(f"    - OPPORTUNITY FOUND: {details}")
                insert_query = "INSERT INTO opportunities (opportunity_type, details) VALUES (%s, %s);"
                cursor.execute(insert_query, ('ARBITRAGE', details))
                db_connection.commit()
        except Exception as e:
            print(f"    - Error analyzing arbitrage for {pair}: {e}")
            db_connection.rollback()
    cursor.close()

def analyze_and_update_sentiment(db_connection):
    """Analyzes sentiment for news headlines that haven't been scored yet."""
    print("  > Analyzing news sentiment...")
    cursor = db_connection.cursor()
    sid = SentimentIntensityAnalyzer()
    cursor.execute("SELECT id, headline FROM market_news WHERE sentiment_score IS NULL;")
    headlines_to_analyze = cursor.fetchall()
    if not headlines_to_analyze:
        print("    - No new headlines to analyze.")
        cursor.close()
        return
    for row in headlines_to_analyze:
        try:
            headline_id, headline_text = row[0], row[1]
            score = sid.polarity_scores(headline_text)['compound']
            update_query = "UPDATE market_news SET sentiment_score = %s WHERE id = %s;"
            cursor.execute(update_query, (score, headline_id))
            print(f"    - Scored headline ID {headline_id} with sentiment {score:.2f}")
        except Exception as e:
            print(f"    - Failed to analyze headline ID {headline_id}: {e}")
            db_connection.rollback()
    db_connection.commit()
    cursor.close()

def train_and_predict_prices(db_connection):
    """Trains a model for each stock and predicts the next 7 days."""
    print("  > Starting AI price prediction cycle...")
    cursor = db_connection.cursor()
    for ticker in STOCKS_TO_PREDICT:
        try:
            print(f"    - Training model for {ticker}...")
            hist_data = yf.download(ticker, period="1y")
            
            if hist_data.empty:
                print(f"    - No historical data found for {ticker} to train on.")
                continue

            # --- THIS IS THE NEW, MORE ROBUST FIX ---
            # 1. Manually create the DataFrame in the format Prophet requires
            df = pd.DataFrame()
            df['ds'] = hist_data.index
            df['y'] = hist_data['Close'].values

            # 2. Clean the data by removing any rows with missing values
            df.dropna(inplace=True)

            # 3. Check if the dataframe is empty AFTER cleaning
            if df.empty:
                print(f"    - No valid historical data for {ticker} after cleaning. Skipping.")
                continue
            # -----------------------------------------

            model = Prophet(daily_seasonality=True)
            model.fit(df)
            future = model.make_future_dataframe(periods=7)
            forecast = model.predict(future)

            print(f"    - Storing 7-day forecast for {ticker}...")
            cursor.execute("DELETE FROM price_predictions WHERE asset_ticker = %s;", (ticker,))
            for index, row in forecast.tail(7).iterrows():
                insert_query = "INSERT INTO price_predictions (asset_ticker, prediction_date, predicted_price) VALUES (%s, %s, %s);"
                cursor.execute(insert_query, (ticker, row['ds'], row['yhat']))
            db_connection.commit()
            
        except Exception as e:
            print(f"    - FAILED to generate forecast for {ticker}: {e}")
            db_connection.rollback()
    cursor.close()

# --- Main Loop ---
if __name__ == "__main__":
    last_prediction_run_date = None
    while True:
        try:
            print(f"\n[{datetime.now()}] Starting analysis cycle...")
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
            
            find_arbitrage_opportunities(conn)
            analyze_and_update_sentiment(conn)
            
            today = date.today()
            if today != last_prediction_run_date:
                train_and_predict_prices(conn)
                last_prediction_run_date = today
            else:
                print("  > Price prediction already run today. Skipping.")
            
            conn.close()
            print("  > Analysis cycle complete.")
            print("  > Waiting for 10 minutes...")
            time.sleep(600)
        except Exception as e:
            print(f"An error occurred in the main analyzer loop: {e}")
            time.sleep(600)