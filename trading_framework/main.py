import os
import csv
from datetime import datetime
from data_loader import load_tickers, fetch_data, resample_to_4h
from strategy import TradingStrategy

def log_signal_to_csv(signal_info):
    """
    Logs signal information to a CSV file named with the current date.
    Appends to the file if it already exists.

    Args:
        signal_info (dict): A dictionary containing the signal details.
    """
    # Generate filename with today's date
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"signals_{date_str}.csv"

    # Define the headers for the CSV file
    headers = ["Ticker", "Date", "Signal", "Price", "RSI", "ATR"]

    # Check if file exists to determine if we need to write headers
    file_exists = os.path.isfile(filename)

    try:
        with open(filename, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)

            if not file_exists:
                writer.writeheader() # Write headers only if the file is new

            writer.writerow(signal_info)
    except IOError as e:
        print(f"Error writing to CSV file: {e}")

def run_strategy():
    """
    Main function to run the trading strategy for all tickers in the scripts file.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    scripts_filepath = os.path.join(base_dir, "scripts.csv")

    tickers = load_tickers(scripts_filepath)
    if not tickers:
        print("No tickers to process. Exiting.")
        return

    # Instantiate the strategy, including the new ATR length
    strategy = TradingStrategy(fast_ema_len=5, slow_ema_len=13, rsi_len=14, atr_len=14)

    print("Running strategy for the following tickers:", tickers)
    print("-" * 40)

    for ticker in tickers:
        print(f"Processing {ticker}...")

        daily_data = fetch_data(ticker, period="2y", interval="1d")
        h1_data = fetch_data(ticker, period="730d", interval="1h")

        if daily_data.empty or h1_data.empty:
            print(f"Could not retrieve sufficient data for {ticker}. Skipping.")
            print("-" * 40)
            continue

        h4_data = resample_to_4h(h1_data)
        result_df = strategy.apply_strategy(daily_data, h4_data)

        if result_df.empty:
            print(f"Could not apply strategy for {ticker}. Skipping.")
            print("-" * 40)
            continue

        latest_data = result_df.iloc[-1]

        signal_found = False
        # Check for BUY Signal
        if latest_data['BUY_SIGNAL']:
            signal_found = True
            signal_info = {
                "Ticker": ticker,
                "Date": latest_data.name.strftime("%Y-%m-%d"),
                "Signal": "BUY",
                "Price": f"{latest_data['Close']:.2f}",
                "RSI": f"{latest_data['D_RSI']:.2f}",
                "ATR": f"{latest_data['D_ATR']:.2f}"
            }
            print(f"*** ALERT: New BUY signal for {ticker} ***")
            print(f"    Date: {signal_info['Date']}, Price: {signal_info['Price']}, RSI: {signal_info['RSI']}, ATR: {signal_info['ATR']}")
            log_signal_to_csv(signal_info)

        # Check for SELL Signal
        if latest_data['SELL_SIGNAL']:
            signal_found = True
            signal_info = {
                "Ticker": ticker,
                "Date": latest_data.name.strftime("%Y-%m-%d"),
                "Signal": "SELL",
                "Price": f"{latest_data['Close']:.2f}",
                "RSI": f"{latest_data['D_RSI']:.2f}", # RSI is a daily value, may not be as relevant for a 4H sell signal
                "ATR": f"{latest_data['D_ATR']:.2f}"  # ATR is a daily value, may not be as relevant for a 4H sell signal
            }
            print(f"*** ALERT: New SELL signal for {ticker} ***")
            print(f"    Date: {signal_info['Date']}, Price: {signal_info['Price']}, RSI: {signal_info['RSI']}, ATR: {signal_info['ATR']}")
            log_signal_to_csv(signal_info)

        if not signal_found:
            print(f"No new signal for {ticker}.")

        print("-" * 40)

if __name__ == "__main__":
    run_strategy()
