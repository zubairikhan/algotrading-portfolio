from datetime import datetime, timedelta
import logging
import os
import numpy as np
import pandas as pd
import config
import helper
from strategies.strategy import Strategy
import mplfinance as mpf

class OpeningRangeBreakoutStrategy(Strategy):
    def __init__(self, data_handler, events, portfolio, cutoff_time):
        
        super().__init__(data_handler, events, portfolio, cutoff_time)
        self.name = 'Opening Range Breakout Strategy'

        self.opening_range_bars = config.opening_range_window_bars
        self.opening_range_minutes = self.opening_range_bars * (self.data_handler.bar_granularity // 60)  # Convert bars to minutes
        self.opening_ranges = self._initialize_opening_ranges()
        self.vwap_data = self._initialize_vwap_data()
        self.stop_loss_margin = config.stop_loss_percentage  # in percentage
        self.risk_reward_ratio = config.reward_risk_ratio  # Take Profit is this value times Stop Loss
        self.stop_loss_price = 0

        
    def post_data_fetch_setup(self):
        for symbol in self.data_handler.symbol_list:
            self.data_handler.all_data[symbol]["opening_range_high"] = np.nan
            self.data_handler.all_data[symbol]["opening_range_low"] = np.nan
            self.data_handler.all_data[symbol]["signal"] = None
            self.data_handler.all_data[symbol]["take_profit"] = np.nan
            self.data_handler.all_data[symbol]["stop_loss"] = np.nan
            self.data_handler.all_data[symbol]["vwap"] = np.nan

    
    # def _update_exit_levels(self, symbol, fill_price):
    #     """
    #     Updates exit levels for a given symbol based on fill price.
    #
    #     Args:
    #         symbol (str): The stock symbol to update exit levels for
    #         fill_price (float or np.nan): The fill price from order execution
    #     """
    #     if np.isnan(fill_price):
    #         # Reset take profit on sell order
    #         take_profit = np.nan
    #     else:
    #         # Calculate take profit based on percentage margin
    #         take_profit = fill_price * (1 + self.take_profit_percentage / 100)
    #
    #     self.exit_levels[symbol]['take_profit'] = take_profit
    #     logging.info(f"Exit levels updated for {symbol}: Take Profit = {take_profit}")

    def _update_take_profit_level(self, symbol, fill_price):
        if np.isnan(fill_price):
            return
        
        stop_loss = self.exit_levels[symbol]['stop_loss']
        if np.isnan(stop_loss):
            return
        
        stop_loss_distance = fill_price - stop_loss
        take_profit_price = fill_price + (stop_loss_distance * self.risk_reward_ratio)
        self.exit_levels[symbol]['take_profit'] = take_profit_price

    def _initialize_opening_ranges(self):
        opening_ranges = {}
        for symbol in self.data_handler.symbol_list:
            opening_ranges[symbol] = {'high': -np.inf, 'low': np.inf}
        return opening_ranges
    
    def _initialize_vwap_data(self):
        vwap_data = {}
        for symbol in self.data_handler.symbol_list:
            vwap_data[symbol] = {'cumulative_tp_volume': 0.0, 'cumulative_volume': 0.0}
        return vwap_data

    def _calculate_vwap(self, symbol, bar):
        """
        Calculate VWAP (Volume Weighted Average Price) for the current bar.
        VWAP = Cumulative(Typical Price * Volume) / Cumulative(Volume)
        Typical Price = (High + Low + Close) / 3
        """
        typical_price = (bar.high + bar.low + bar.close) / 3
        self.vwap_data[symbol]['cumulative_tp_volume'] += typical_price * bar.volume
        self.vwap_data[symbol]['cumulative_volume'] += bar.volume

        if self.vwap_data[symbol]['cumulative_volume'] > 0:
            vwap = self.vwap_data[symbol]['cumulative_tp_volume'] / self.vwap_data[symbol]['cumulative_volume']
        else:
            vwap = np.nan

        return vwap

    def _check_entry_condition(self, symbol, bar, opening_range_high, vwap):
        """
        Check if entry conditions are met for a breakout trade.

        Args:
            symbol: Stock symbol
            bar: Current bar data
            opening_range_high: The high of the opening range
            vwap: Current VWAP value

        Returns:
            bool: True if entry conditions are met, False otherwise
        """
        # Already in position
        if self.bought[symbol]:
            return False

        # Check breakout above opening range high
        if bar.high <= opening_range_high:
            return False

        # Apply VWAP filter if enabled
        if config.enable_vwap_entry_condition and bar.close <= vwap:
            return False

        return True

    #TODO: implement for real-time trading
    def on_order_filled(self, symbol, direction, fill_price):
        pass

    def process_start_of_new_day(self):
        self.opening_ranges = self._initialize_opening_ranges()
        self.vwap_data = self._initialize_vwap_data()


    def calculate_signals(self, event):
        if event.type == 'MARKET':
            for symbol in self.data_handler.symbol_list_active:

                five_sec_bar, is_new_bar, bar = self.fetch_latest_data(symbol, 1)

                if five_sec_bar is not None:
                    logging.info(f'5-sec-bar - Time: {five_sec_bar.datetime}, Symbol: {symbol}, Close: {five_sec_bar.close}')

                if bar is None:
                    continue
                
                bar = bar[-1]  # Get the latest bar

                # Calculate and store VWAP for current bar
                vwap = self._calculate_vwap(symbol, bar)
                self.add_property_for_plotting(symbol, bar.datetime, "vwap", vwap)

                # Check for market closing time
                # If past cutoff time, sell all positions
                if helper.is_trading_cutoff_time(bar.datetime):
                    logging.info(f"Symbol: {symbol} - Market Closing -  Time: {bar.datetime}, Latest close: {bar.close}\n")

                    quantity = self.portfolio.current_positions[symbol]
                    if quantity > 0:
                        sell_price = bar.close
                        signal = self.sell(symbol, bar.datetime, sell_price, quantity, 'MKT CLOSING - CUT OFF TIME REACHED')
                        self.events.put(signal)
                    continue

                current_time = bar.datetime

                # Check if we're still within the opening range period
                mkt_open_time = datetime.strptime(config.mkt_open_time, "%H:%M:%S")
                market_open = current_time.replace(hour=mkt_open_time.hour, minute=mkt_open_time.minute, second=0, microsecond=0)
                opening_range_end = market_open + timedelta(minutes=self.opening_range_minutes)

                # Update opening range if we're still within the opening period
                if current_time < opening_range_end:
                    self.opening_ranges[symbol]['high'] = max(self.opening_ranges[symbol]['high'], bar.high)
                    self.opening_ranges[symbol]['low'] = min(self.opening_ranges[symbol]['low'], bar.low)

                    opening_range_low = self.opening_ranges[symbol]['low']
                    self.exit_levels[symbol]['stop_loss'] = opening_range_low * (1 - self.stop_loss_margin / 100)
                    continue

                # Skip if opening range hasn't been established
                if self.opening_ranges[symbol]['high'] == -np.inf or self.opening_ranges[symbol]['low'] == np.inf:
                    continue

                self.add_properties_for_plotting(symbol, bar)
                
                opening_range_high = self.opening_ranges[symbol]['high']

                # Check for breakout above opening range high
                if self._check_entry_condition(symbol, bar, opening_range_high, vwap):
                    buy_price = bar.close
                    self._update_take_profit_level(symbol, buy_price)
                    signal = self.buy(symbol, bar.datetime, buy_price, 2, f"Opening Range Breakout - High: {bar.high} > Opening Range High: {opening_range_high}")
                    self.events.put(signal)

                elif self.bought[symbol]:
                    stop_loss = self.exit_levels[symbol]['stop_loss']
                    take_profit = self.exit_levels[symbol]['take_profit']

                    quantity = self.portfolio.current_positions.get(symbol, 0)

                    if bar.low <= stop_loss:
                        signal = self.sell(symbol, bar.datetime, stop_loss, quantity, "Stop loss hit")
                        self.events.put(signal)
                        self.exit_levels[symbol]['take_profit'] = np.nan
                    elif bar.high >= take_profit:
                        signal = self.sell(symbol, bar.datetime, take_profit, quantity, "Take profit hit")
                        self.events.put(signal)
                        self.exit_levels[symbol]['take_profit'] = np.nan

    
    def add_properties_for_plotting(self, symbol, bar):
        self.add_property_for_plotting(symbol, bar.datetime, "opening_range_high", self.opening_ranges[symbol]['high'])
        self.add_property_for_plotting(symbol, bar.datetime, "opening_range_low", self.opening_ranges[symbol]['low'])
        self.add_property_for_plotting(symbol, bar.datetime, "stop_loss", self.exit_levels[symbol]['stop_loss'])
        self.add_property_for_plotting(symbol, bar.datetime, "take_profit", self.exit_levels[symbol]['take_profit'])
                
    
    def plot(self):
        pass
    

    def plot_daily_candlestick(self, df, symbol, output_dir):
        # Ensure datetime is datetime type and set as index
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)

        # Group by day
        for day, day_data in df.groupby(df.index.date):
            if (day_data['signal'].isna().all()):
                continue
            # Prepare OHLCV data
            ohlcv = day_data[['open', 'high', 'low', 'close', 'volume']]

            # Create additional plots            
            add_plots = [
                mpf.make_addplot(day_data['opening_range_high'], type='line', color='black', linestyle=':', width=0.8),
                mpf.make_addplot(day_data['opening_range_low'], type='line', color='black', linestyle=':', width=0.8),
                mpf.make_addplot(day_data['take_profit'], type='line', color='blue', linestyle='--', width=0.8),
                mpf.make_addplot(day_data['stop_loss'], type='line', color='red', linestyle='--', width=0.8),
                mpf.make_addplot(day_data['vwap'], type='line', color='purple', linestyle='-', width=1.2),
            ]

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


