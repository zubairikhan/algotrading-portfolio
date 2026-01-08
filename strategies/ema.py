import logging
import numpy as np
from ta import trend
from helper import is_trading_cutoff_time
import pandas as pd
import helper
import os
import shutil
import mplfinance as mpf
import config
from strategies.strategy import Strategy

class EMAStrategy(Strategy):

    def __init__(self, data_handler, events, portfolio, cutoff_time):

        super().__init__(data_handler, events, portfolio, cutoff_time)
        
        self.name = 'EMA Strategy'
        self.short_period = config.ema_short_period
        self.long_period = config.ema_long_period
        self.take_profit_percentage = config.take_profit_percentage

        self.use_rsi = config.enable_rsi_indicator
        self.rsi_period = config.rsi_period
        self.rsi_overbought = config.rsi_overbought
        self.rsi_oversold = config.rsi_oversold

        self.stocks_sold_at_mkt_closing = []
        self.stocks_to_retain = []
        
        logging.info(f"Initialized EMA Strategy with Short Period: {self.short_period}, Long Period: {self.long_period}, Take Profit Margin: {self.take_profit_percentage}%\n")
        if self.use_rsi:
            logging.info(f"RSI Indicator Enabled with Period: {self.rsi_period}, Overbought: {self.rsi_overbought}, Oversold: {self.rsi_oversold}\n")

    def post_data_fetch_setup(self):
        for symbol in self.data_handler.symbol_list:
            self.data_handler.all_data[symbol]["ema_short"] = 0
            self.data_handler.all_data[symbol]["ema_long"] = 0
            self.data_handler.all_data[symbol]["signal"] = None
            self.data_handler.all_data[symbol]["take_profit"] = np.nan

    def on_order_filled(self, symbol, direction, fill_price):
        if direction == 'BUY':
            self._update_exit_levels(symbol, fill_price)
        elif direction == 'SELL':
            self._update_exit_levels(symbol, np.nan)

    def _update_exit_levels(self, symbol, fill_price):
        """
        Updates exit levels for a given symbol based on fill price.
        
        Args:
            symbol (str): The stock symbol to update exit levels for
            fill_price (float or np.nan): The fill price from order execution
        """
        if np.isnan(fill_price):
            # Reset take profit on sell order
            take_profit = np.nan
        else:
            # Calculate take profit based on percentage margin
            take_profit = fill_price * (1 + self.take_profit_percentage / 100)
        
        self.exit_levels[symbol]['take_profit'] = take_profit
        logging.info(f"Exit levels updated for {symbol}: Take Profit = {take_profit}")

    def calculate_moving_averages(self, df):
        df['EMA_short'] = trend.ema_indicator(df['close'], window=self.short_period)
        df['EMA_long'] = trend.ema_indicator(df['close'], window=self.long_period)
        return df

    def calculate_rsi(self, df):
        """Calculate the RSI indicator for a given set of data."""
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_period).mean()
        rs = gain / loss
        ratio = 100 - (100 / (1 + rs))
        df['RSI'] = ratio
        return df

    def calculate_signals(self, event):
        if event.type == 'MARKET':

            five_sec_bar = None
            for symbol in self.data_handler.symbol_list_active:

                five_sec_bar, is_new_bar, data = self.fetch_latest_data(symbol)

                if five_sec_bar is not None:
                    logging.info(f'5-sec-bar - Time: {five_sec_bar.datetime}, Symbol: {symbol}, Close: {five_sec_bar.close}')

                if len(data) == 0:
                    continue

                df = self.convert_raw_date_to_dataframe(data)

                df = self.calculate_moving_averages(df)

                if self.use_rsi:
                    df = self.calculate_rsi(df)

                latest = df.iloc[-1]

                self.add_properties_for_plotting(symbol, latest)

                ## Buy Previous Day Sold Stocks
                if helper.is_new_day(self.data_handler) and symbol in self.stocks_to_retain:
                    if latest['EMA_short'] > latest['EMA_long']:
                        quantity = 2
                        buy_price = latest['close']
                        signal = self.buy(symbol, latest['date'], buy_price, quantity, 'PREVIOUS DAY CLOSED STOCKS RETAINED AT DAY START')
                        self.events.put(signal)
                        if helper.IS_BACKTEST:
                            self._update_exit_levels(symbol, buy_price)
                        continue
                    
                    self.stocks_to_retain.remove(symbol)

                # Check for market closing time
                # If past cutoff time, sell all positions
                if is_trading_cutoff_time(latest['date']):
                    logging.info(f"Symbol: {symbol} - Market Closing -  Time: {latest['date']}, Latest close: {latest['close']}\n")

                    quantity = self.portfolio.current_positions[symbol]
                    if quantity > 0:
                        sell_price = latest['close']
                        signal = self.sell(symbol, latest['date'], sell_price, quantity, 'MKT CLOSING - CUT OFF TIME REACHED')
                        self.events.put(signal)
                        self._update_exit_levels(symbol, np.nan)
                        self.save_stocks_sold_at_mkt_closing(symbol)
                    continue

                # Check for exit strategy (take profit)
                if self.bought[symbol]:
                    signal = self.process_exit_strategy(symbol, self.data_handler.get_latest_data(symbol)[0])
                    if signal is not None:
                        self.events.put(signal)
                        continue

                # Run EMA strategy
                if data is not None and is_new_bar and len(data) >= self.long_period:
                    exit_levels = self.exit_levels[symbol]
                    try:
                        value = (str(five_sec_bar.datetime), five_sec_bar.close)
                    except AttributeError:
                        value = None

                    logging.info(
                        f"Symbol: {symbol}, 5-Sec-Bar: [{value}] | Aggregated Bar - Time: {latest['date']}, Open:{latest['open']}, High: {latest['high']}, Low: {latest['low']}, Close: {latest['close']}, Volume: {latest['volume']} "
                        f"EMA_short: {latest['EMA_short']}, EMA_long: {latest['EMA_long']}, RSI: {latest['RSI']} StopLoss: {exit_levels['stop_loss']}, TakeProfit: {exit_levels['take_profit']}")

                    if self.is_buying_condition_met(symbol, df):
                        quantity = 2
                        buy_price = latest['close']
                        signal = self.buy(symbol, latest['date'], buy_price, quantity, f"Short-term EMA {latest['EMA_short']} crossed ABOVE Long-term EMA {latest['EMA_long']}")
                        self.events.put(signal)
                        if helper.IS_BACKTEST:
                            self._update_exit_levels(symbol, buy_price)

                    elif self.is_selling_condition_met(symbol, df):
                        quantity = self.portfolio.current_positions[symbol]
                        if quantity > 0:
                            sell_price = latest['close']
                            signal = self.sell(symbol, latest['date'], sell_price, quantity, f"Short-term EMA {latest['EMA_short']} crossed BELOW Long-term EMA {latest['EMA_long']}")
                            self.events.put(signal)
                            self._update_exit_levels(symbol, np.nan)
                            
    def is_buying_condition_met(self, symbol, df):
        return self.bought[symbol] == False and self.has_short_term_ema_crossed_above_long_term_ema(df) and (not self.use_rsi or self.is_rsi_oversold(df))

    def is_selling_condition_met(self, symbol, df):
        return self.bought[symbol] == True and self.has_short_term_ema_crossed_below_long_term_ema(df) and (not self.use_rsi or self.is_rsi_overbought(df))

    def convert_raw_date_to_dataframe(self, data):
        df = pd.DataFrame(data, columns=["symbol", "date", "open", "high", "low", "close", "volume"])
        df = df.drop(["symbol"], axis=1)
        df.set_index('date', inplace=True, drop=False)

        return df

    def add_properties_for_plotting(self, symbol, latest):
        dt = pd.Timestamp(latest['date'])
        all_data_df = self.data_handler.all_data[symbol]
        all_data_df.loc[all_data_df['date'] == dt, "ema_short"] = latest['EMA_short']
        all_data_df.loc[all_data_df['date'] == dt, "ema_long"] = latest['EMA_long']
        if self.use_rsi:
            all_data_df.loc[all_data_df['date'] == dt, "rsi"] = latest['RSI']
        all_data_df.loc[all_data_df['date'] == dt, "take_profit"] = self.exit_levels[symbol]['take_profit']
        return dt,all_data_df
    

    def has_short_term_ema_crossed_above_long_term_ema(self, df):
        prev = df.iloc[-2]
        curr = df.iloc[-1]

        if prev['EMA_short'] < prev['EMA_long'] and curr['EMA_short'] > curr['EMA_long']:
            logging.info(
                f"EMA CROSSED ABOVE - PrevEMA_Short: {prev['EMA_short']}, PrevEMA_long: {prev['EMA_long']}, CurrEMA_short: {curr['EMA_short']}, CurrEMA_long: {curr['EMA_long']}")
            return True

        return False

    def has_short_term_ema_crossed_below_long_term_ema(self, df):
        prev = df.iloc[-2]
        curr = df.iloc[-1]

        if prev['EMA_short'] > prev['EMA_long'] and curr['EMA_short'] < curr['EMA_long']:
            logging.info(
                f"EMA CROSSED BELOW - PrevEMA_Short: {prev['EMA_short']}, PrevEMA_long: {prev['EMA_long']}, CurrEMA_short: {curr['EMA_short']}, CurrEMA_long: {curr['EMA_long']}")
            return True

        return False

    def is_rsi_overbought(self, df):
        return df.iloc[-1]['RSI'] > self.rsi_overbought

    def is_rsi_oversold(self, df):
        return df.iloc[-1]['RSI'] < self.rsi_oversold

    def process_exit_strategy(self, symbol, bar):
        quantity = self.portfolio.current_positions[symbol]
        take_profit = self.exit_levels[symbol]['take_profit']
        stop_loss = self.exit_levels[symbol]['stop_loss']

        exit_condition_met = bar.high > take_profit

        if not exit_condition_met:
            return None
        
        logging.info(
            f"Symbol: {symbol}, Time: {bar.datetime}, Open: {bar.open}, High: {bar.high}, Low: {bar.low}, Close: {bar.close}, Volume: {bar.volume} "
            f"EMA_short: [Uncomputed], EMA_long: [Uncomputed], RSI:[Uncomputed], StopLoss: {stop_loss}, TakeProfit: {take_profit}")
        
        sell_price = take_profit
        signal = self.sell(symbol, bar.datetime, sell_price, quantity, f"High {bar.high} crossed Take Profit {take_profit}")
        self._update_exit_levels(symbol, np.nan)
        return signal

    def process_start_of_new_day(self):
        self.initialize_stocks_to_retain_from_prev_day()

    def initialize_stocks_to_retain_from_prev_day(self):
        if not helper.IS_BACKTEST:
            self.stocks_sold_at_mkt_closing = self.get_symbols_sold_at_prev_mkt_closing()

        for symbol in self.stocks_sold_at_mkt_closing:
            if symbol in self.data_handler.symbol_list_active:
                self.stocks_to_retain.append(symbol)
        self.stocks_sold_at_mkt_closing = []

    def get_symbols_sold_at_prev_mkt_closing(self):
        stock_sold_at_mkt_closing_file = []
        if os.path.exists("stocks_sold_at_mkt_closing.csv"):
            with open("stocks_sold_at_mkt_closing.csv", "r") as file:
                stock_sold_at_mkt_closing_file = [line.strip() for line in file.readlines()]
            os.remove("stocks_sold_at_mkt_closing.csv")
        return stock_sold_at_mkt_closing_file

    def save_stocks_sold_at_mkt_closing(self, symbol):
        self.stocks_sold_at_mkt_closing.append(symbol)
        if not helper.IS_BACKTEST:
            # Write to file stocks which were sold at mkt closing
            with open("stocks_sold_at_mkt_closing.csv", "a") as file:
                file.write(f"{symbol}\n")
    

    def plot(self):
        pass


    def plot_daily_candlestick(self, df, symbol, output_dir):
        df["ema_short"] = df["ema_short"].fillna(0)
        df["ema_long"] = df["ema_long"].fillna(0)

        # Ensure datetime is datetime type and set as index
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)

        # Group by day
        for day, day_data in df.groupby(df.index.date):
            if (day_data['signal'].isna().all()):
                continue
            # Prepare OHLCV data
            ohlcv = day_data[['open', 'high', 'low', 'close', 'volume']]

            # EMA lines
            add_plots = [
                mpf.make_addplot(day_data['ema_short'], color='blue', linestyle='--', width=0.8),
                mpf.make_addplot(day_data['ema_long'], color='red', linestyle='--', width=0.8),
                mpf.make_addplot(day_data['take_profit'], type='line', color='black', linestyle='--', width=0.8),
            ]

            if self.use_rsi:
                add_plots.append(mpf.make_addplot(day_data['rsi'], panel=2, color='purple', linestyle='-', width=0.8, ylabel='RSI'))
                add_plots.append(mpf.make_addplot([self.rsi_overbought]*len(day_data), panel=2, color='black', linestyle='--', width=0.8))
                add_plots.append(mpf.make_addplot([self.rsi_oversold]*len(day_data), panel=2, color='black', linestyle='--', width=0.8))

            # Buy signals (green upward triangle)
            buy_signals = day_data['close'].where(day_data['signal'] == 'BUY')
            add_plots.append(mpf.make_addplot(buy_signals, type='scatter', markersize=50, marker='^', color='cyan'))

            # Sell signals (red downward triangle)
            sell_signals = day_data['close'].where(day_data['signal'] == 'SELL')
            add_plots.append(mpf.make_addplot(sell_signals, type='scatter', markersize=50, marker='v', color='magenta'))

            try:
                # Plot
                mpf.plot(
                    ohlcv,
                    type='candle',
                    volume=True,
                    addplot=add_plots,
                    title=f"OHLCV Chart for {day}-{symbol}",
                    style='yahoo',
                    savefig=os.path.join(output_dir, f"{day}_{symbol}" + ".png"),
                )
            except Exception as e:
                logging.error(f"Error plotting candlestick for {symbol} on {day}: {e}")
