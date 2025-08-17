import os
from data_loader import load_tickers, fetch_data, resample_to_4h
from strategy import TradingStrategy

def run_strategy():
    """
    Main function to run the trading strategy for all tickers in the scripts file.
    """
    # Define the path to the scripts.csv file, assuming it's in the same directory
    # This makes the script runnable from anywhere
    base_dir = os.path.dirname(os.path.abspath(__file__))
    scripts_filepath = os.path.join(base_dir, "scripts.csv")

    tickers = load_tickers(scripts_filepath)
    if not tickers:
        print("No tickers to process. Exiting.")
        return

    # Instantiate the strategy
    strategy = TradingStrategy(fast_ema_len=5, slow_ema_len=13, rsi_len=14)

    print("Running strategy for the following tickers:", tickers)
    print("-" * 30)

    for ticker in tickers:
        print(f"Processing {ticker}...")

        # 1. Fetch data for both timeframes
        # Fetch 2 years of daily data to have enough history for indicators
        daily_data = fetch_data(ticker, period="2y", interval="1d")
        # Fetch 730 days (max for 1h interval) of hourly data
        h1_data = fetch_data(ticker, period="730d", interval="1h")

        if daily_data.empty or h1_data.empty:
            print(f"Could not retrieve sufficient data for {ticker}. Skipping.")
            print("-" * 30)
            continue

        # 2. Resample 1-hour data to 4-hour
        h4_data = resample_to_4h(h1_data)

        # 3. Apply the strategy
        result_df = strategy.apply_strategy(daily_data, h4_data)

        if result_df.empty:
            print(f"Could not apply strategy for {ticker}. Skipping.")
            print("-" * 30)
            continue

        # 4. Check for the latest signal
        latest_signal = result_df.iloc[-1]

        signal_found = False
        if latest_signal['BUY_SIGNAL']:
            print(f"*** ALERT: New BUY signal for {ticker} on {latest_signal.name.date()} ***")
            signal_found = True

        if latest_signal['SELL_SIGNAL']:
            print(f"*** ALERT: New SELL signal for {ticker} on {latest_signal.name.date()} ***")
            signal_found = True

        if not signal_found:
            print(f"No new signal for {ticker}.")

        print("-" * 30)

if __name__ == "__main__":
    run_strategy()
