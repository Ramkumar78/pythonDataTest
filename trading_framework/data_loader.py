import pandas as pd
import yfinance as yf

def load_tickers(filepath="scripts.csv"):
    """
    Loads a list of tickers from a CSV file.
    The CSV file should have a header, and the tickers should be in a column named 'TICKER'.

    Args:
        filepath (str): The path to the CSV file.

    Returns:
        list: A list of ticker symbols.
    """
    try:
        df = pd.read_csv(filepath)
        if "TICKER" not in df.columns:
            print("Error: CSV file must contain a 'TICKER' column.")
            return []
        return df["TICKER"].dropna().unique().tolist()
    except FileNotFoundError:
        print(f"Error: The file '{filepath}' was not found.")
        return []

def fetch_data(ticker, period="1y", interval="1d"):
    """
    Fetches historical market data for a given ticker from Yahoo Finance.

    Args:
        ticker (str): The stock ticker symbol.
        period (str): The period for which to fetch data (e.g., "1y", "6mo").
        interval (str): The data interval (e.g., "1d", "1h").

    Returns:
        pd.DataFrame: A DataFrame containing the OHLCV data, or an empty DataFrame if fetching fails.
    """
    stock = yf.Ticker(ticker)
    data = stock.history(period=period, interval=interval)
    if data.empty:
        print(f"Could not download data for {ticker}. It might be delisted or an invalid ticker.")
    return data

def resample_to_4h(df_1h):
    """
    Resamples 1-hour OHLCV data to 4-hour data.

    Args:
        df_1h (pd.DataFrame): DataFrame with 1-hour frequency data.

    Returns:
        pd.DataFrame: A new DataFrame with 4-hour resampled data.
    """
    if df_1h.empty:
        return pd.DataFrame()

    # Define the aggregation logic for resampling
    aggregation = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }

    # Resample the data to a 4-hour frequency
    df_4h = df_1h.resample('4h').agg(aggregation)

    # Drop rows where all values are NaN (which occur for non-trading hours)
    df_4h.dropna(how='all', inplace=True)

    return df_4h
