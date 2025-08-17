import pandas as pd
import pandas_ta as ta

class TradingStrategy:
    """
    A class to encapsulate the trading strategy logic based on the provided Pine Script.
    """
    def __init__(self, fast_ema_len=5, slow_ema_len=13, rsi_len=14, rsi_threshold=50):
        """
        Initializes the strategy with given parameters.
        """
        self.fast_ema_len = fast_ema_len
        self.slow_ema_len = slow_ema_len
        self.rsi_len = rsi_len
        self.rsi_threshold = rsi_threshold

    def _calculate_indicators(self, daily_data, h4_data):
        """
        Calculates all the necessary technical indicators for the strategy.
        """
        # Calculate Daily indicators
        daily_data.ta.ema(length=self.fast_ema_len, append=True, col_names=('D_EMA_fast',))
        daily_data.ta.ema(length=self.slow_ema_len, append=True, col_names=('D_EMA_slow',))
        daily_data.ta.rsi(length=self.rsi_len, append=True, col_names=('D_RSI',))

        # Calculate 4-Hour indicators
        h4_data.ta.ema(length=self.fast_ema_len, append=True, col_names=('H4_EMA_fast',))
        h4_data.ta.ema(length=self.slow_ema_len, append=True, col_names=('H4_EMA_slow',))

        return daily_data, h4_data

    def apply_strategy(self, daily_data, h4_data):
        """
        Applies the trading strategy to the given data.

        Args:
            daily_data (pd.DataFrame): The daily market data.
            h4_data (pd.DataFrame): The 4-hour market data.

        Returns:
            pd.DataFrame: The 4-hour DataFrame with 'BUY_SIGNAL' and 'SELL_SIGNAL' columns.
        """
        if daily_data.empty or h4_data.empty:
            print("Warning: DataFrames are empty. Cannot apply strategy.")
            return pd.DataFrame()

        daily_data, h4_data = self._calculate_indicators(daily_data, h4_data)

        # Align daily data to 4-hour data by reindexing
        # We forward-fill to ensure the daily signal persists throughout the day
        daily_signals = daily_data[['D_EMA_fast', 'D_EMA_slow', 'D_RSI']].reindex(h4_data.index, method='ffill')

        # --- Generate Buy Signals (Daily TF) ---
        # Condition 1: Daily Fast EMA crosses above Daily Slow EMA
        buy_crossover = (daily_signals['D_EMA_fast'].shift(1) < daily_signals['D_EMA_slow'].shift(1)) & \
                        (daily_signals['D_EMA_fast'] > daily_signals['D_EMA_slow'])
        # Condition 2: Daily RSI is above the threshold
        rsi_condition = daily_signals['D_RSI'] > self.rsi_threshold

        h4_data['BUY_SIGNAL'] = buy_crossover & rsi_condition

        # --- Generate Sell Signals (4H TF) ---
        # Condition: 4H Fast EMA crosses below 4H Slow EMA
        sell_crossunder = (h4_data['H4_EMA_fast'].shift(1) > h4_data['H4_EMA_slow'].shift(1)) & \
                          (h4_data['H4_EMA_fast'] < h4_data['H4_EMA_slow'])

        h4_data['SELL_SIGNAL'] = sell_crossunder

        return h4_data
