import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from prophet import Prophet
from prophet.plot import plot_plotly
from datetime import date

# --- App Configuration ---
st.set_page_config(
    page_title="AI Trend Forecaster",
    page_icon="�",
    layout="wide"
)

# --- Database Connection Details ---
DB_NAME = "fin_data_db"
DB_USER = "postgres"
DB_PASS = "Piya082001@"  # <<< PASTE YOUR PASSWORD HERE
DB_HOST = "127.0.0.1"
DB_PORT = "5432"

# --- App Title and Description ---
st.title('🔮 AI Trend Forecaster')
st.write('This app uses your collected market data to forecast future price trends.')
st.write('---')

# --- Sidebar for User Input ---
st.sidebar.header('User Input')
assets = ('RELIANCE.NS', 'TCS.NS', 'BTCINR', 'ETHINR', 'MATICINR')
selected_asset = st.sidebar.selectbox('Select an asset for prediction', assets)

# --- THIS IS THE NEW, EFFICIENT FUNCTION ---
@st.cache_data
def generate_forecast(ticker):
    """
    Loads data, trains the model, and returns the forecast.
    This entire function is cached for performance.
    """
    try:
        # 1. Create a database engine using SQLAlchemy with the explicit driver
        # --- THIS IS THE FINAL FIX ---
        db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)
        
        query = f"SELECT timestamp, price FROM price_data WHERE asset_ticker = '{ticker}';"
        
        # 2. Use the engine with pandas
        df = pd.read_sql_query(query, engine)

        if df.empty:
            return None, None

        # 3. Prepare data for Prophet
        df_train = df[['timestamp', 'price']].rename(columns={"timestamp": "ds", "price": "y"})

        # Remove the timezone information from the 'ds' column
        df_train['ds'] = df_train['ds'].dt.tz_localize(None)

        # 4. Create and train the Prophet model
        m = Prophet(daily_seasonality=True)
        m.fit(df_train)

        # 5. Create a dataframe for future dates and predict
        future = m.make_future_dataframe(periods=7)
        forecast = m.predict(future)
        
        return m, forecast

    except Exception as e:
        st.error(f"An error occurred during forecasting: {e}")
        return None, None

# --- Main App Logic ---
forecast_load_state = st.sidebar.text(f'Generating forecast for {selected_asset}...')
model, forecast = generate_forecast(selected_asset)
forecast_load_state.text(f'Forecast for {selected_asset} is ready!')


if forecast is not None:
    # --- Display Forecast Results ---
    st.subheader('Forecast Data')
    st.write(f'7-day forecast for {selected_asset}')
    st.write(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(7))

    # --- Plotting the Forecast ---
    st.subheader('Forecast Visualization')
    fig1 = plot_plotly(model, forecast)
    fig1.update_layout(
        title_text=f'Forecast Plot for {selected_asset}',
        xaxis_title='Date',
        yaxis_title='Price'
    )
    st.plotly_chart(fig1, use_container_width=True)

    # --- Plotting Forecast Components ---
    if st.sidebar.checkbox('Show Forecast Components'):
        st.subheader("Forecast Components")
        fig2 = model.plot_components(forecast)
        st.write(fig2)
else:
    st.warning(f"Could not generate a forecast for {selected_asset}. Please ensure your data harvester script is running and has collected enough data.")

# --- Disclaimer ---
st.sidebar.write('---')
st.sidebar.warning('Disclaimer: This tool is for educational purposes only and should not be used for real investment decisions.')