from datetime import datetime, timedelta
import logging
import os
from statistics import mean
import pandas as pd
import helper
from enum import Enum
import config

class DailyPerformanceCriteria(Enum):
    Strong = 1,
    Weak = 2

class StockFilter:
    def __init__(self, data_handler, cutoff_time, sma_long_period, sma_short_period, bar_granularity):
        self.data_handler = data_handler
        self.sma_short_period = sma_short_period
        self.sma_long_period = sma_long_period
        self.cutoff_time = cutoff_time
        self.bar_granularity = bar_granularity
        self.float_limit = config.filter_float_limit
        self.volume_days = config.filter_volume_days
        self.volume_multiple = config.filter_volume_multiplier
        self.sma_close_multiplier = config.filter_sma_close_multiplier
        self.daily_performance_criteria = DailyPerformanceCriteria[config.filter_daily_performance_criteria]
        self.gap_up_percentage = config.filter_gap_up_percentage
        self.is_backtest = helper.IS_BACKTEST


    
    def float_filter(self):
        filtered_tickers = []
        for ticker, data in self.data_handler.fundamental_data.items():
            if data["float"] is not None and float(data["float"]) < self.float_limit:
                filtered_tickers.append(ticker)

        logging.info(f"After float filter, {len(filtered_tickers)} stocks remain.")
        return filtered_tickers

    def relative_volume_filter_for_backtesting(self, tickers):
        filtered_tickers = {}
        for ticker, values in tickers.items():
            data = self.data_handler.get_latest_data(ticker, 0)
            #data = self.data_handler.get_latest_data_aggregated(ticker, 0)

            passed_filter, latest_volume, avg_volume_scaled = self.relative_volume_filter(data)
            if passed_filter:
                filtered_tickers[ticker] = values
                filtered_tickers[ticker]["latest_volume"] = latest_volume
                filtered_tickers[ticker]["avg_volume_scaled"] = avg_volume_scaled
            
        return filtered_tickers
    
    def relative_volume_filter_for_live_trading(self, tickers):
        filtered_tickers = {}
        for ticker, values in tickers.items():
            data = self.data_handler.get_latest_data_aggregated(ticker, 0)
            passed_filter, latest_volume, avg_volume_scaled = self.relative_volume_filter(data)
            if passed_filter:
                filtered_tickers[ticker] = values
                filtered_tickers[ticker]["latest_volume"] = latest_volume
                filtered_tickers[ticker]["avg_volume_scaled"] = avg_volume_scaled
        
        logging.info(f"Relative volume filter reduced to {len(filtered_tickers.keys())} stocks")

        logging.info("New Day - Trading on %s stocks: %s", len(filtered_tickers.keys()), list(filtered_tickers.keys()))
        for ticker, values in filtered_tickers.items():
            logging.info(f"{ticker}: {values}")
        
        return filtered_tickers

    def relative_volume_filter(self, data):
        latest = data[-1]
        data = data[:-1]
        filtered_data = [item for item in data if item.datetime.time() == latest.datetime.time()]
        filtered_data = filtered_data[-1 * self.volume_days:]
        if filtered_data:
            avg_volume = mean(item.volume for item in filtered_data)
        else:
            avg_volume = 0
        latest_volume = latest.volume
        avg_volume_scaled = avg_volume * self.volume_multiple
        if latest_volume >= avg_volume_scaled:
            return True, latest_volume, avg_volume
        return False, latest_volume, avg_volume_scaled

    def daily_performance_filter_for_backtesting(self, tickers):
        # Last daily closing price is higher than 20 day SMA and 50 day SMA
        filtered_tickers = {}
        for ticker in tickers:
            data = self.data_handler.get_latest_data(ticker, 0)
            #data = self.data_handler.get_latest_data_aggregated(ticker, 0)
            passed_filter, last_daily_close, last_daily_close_sma_short, last_daily_close_sma_long = self.daily_performance_filter(data)
            if passed_filter:
                filtered_tickers[ticker] = {
                    "last_daily_close": last_daily_close,
                    "close_sma_short": last_daily_close_sma_short,
                    "close_sma_long": last_daily_close_sma_long
                }

        return filtered_tickers
    
    def daily_performance_filter_for_live_trading(self, tickers):
    # Last daily closing price is higher than 20 day SMA and 50 day SMA
        filtered_tickers = {}
        for ticker in tickers:
            data = self.data_handler.get_latest_data_aggregated(ticker, 0)
            passed_filter, last_daily_close, last_daily_close_sma_short, last_daily_close_sma_long = self.daily_performance_filter(data)
            if passed_filter:
                filtered_tickers[ticker] = {
                    "last_daily_close": last_daily_close,
                    "close_sma_short": last_daily_close_sma_short,
                    "close_sma_long": last_daily_close_sma_long
                }

        logging.info(f"Daily performance filter ({self.daily_performance_criteria})reduced to {len(filtered_tickers.keys())} stocks: {list(filtered_tickers.keys())}")
        return filtered_tickers

    def daily_performance_filter(self, data):
        filtered_data = [item for item in data if item.datetime.time() == (datetime.strptime(helper.MKT_CLOSE_TIME, '%H:%M:%S') - timedelta(minutes=self.bar_granularity/60)).time()]
        df = pd.DataFrame(filtered_data, columns=["symbol", "date", "open", "high", "low", "close", "volume"])
        df.drop(["symbol", "open", "high", "low", "volume"], axis=1)
        df.set_index('date', inplace=True, drop=False)
        df = helper.calculate_sma(df, 'close', 'sma_short', self.sma_short_period)
        df = helper.calculate_sma(df, 'close', 'sma_long', self.sma_long_period)
        last_daily_closing_bar = df.iloc[-1]
        last_daily_close = last_daily_closing_bar["close"]
        last_daily_close_sma_short = last_daily_closing_bar["sma_short"] * self.sma_close_multiplier
        last_daily_close_sma_long = last_daily_closing_bar["sma_long"] * self.sma_close_multiplier

        if self.daily_performance_criteria == DailyPerformanceCriteria.Strong:

            if last_daily_close > last_daily_close_sma_short and last_daily_close > last_daily_close_sma_long:
                return True, last_daily_close, last_daily_close_sma_short, last_daily_close_sma_long
        
        elif self.daily_performance_criteria == DailyPerformanceCriteria.Weak:
            if last_daily_close < last_daily_close_sma_short and last_daily_close < last_daily_close_sma_long:
                return True, last_daily_close, last_daily_close_sma_short, last_daily_close_sma_long
        
        return False, None, None, None

    def gap_up_filter(self, data):
        curr_opening_bar = data[-1]
        prev_closing_bar = data[-2]
        
        gap_up_threshold = prev_closing_bar.close * (1 + self.gap_up_percentage / 100)

        if curr_opening_bar.open > gap_up_threshold:
            return True, curr_opening_bar.open, gap_up_threshold

        return False, None, None

    def gap_up_filter_for_backtesting(self, tickers):
        filtered_tickers = {}
        for ticker, values in tickers.items():
            data = self.data_handler.get_latest_data(ticker, 0)
            #data = self.data_handler.get_latest_data_aggregated(ticker, 0)

            passed_filter, open_price, gap_up_threshold = self.gap_up_filter(data)
            if passed_filter:
                filtered_tickers[ticker] = values
                filtered_tickers[ticker]["open_price"] = open_price
                filtered_tickers[ticker]["gap_up_threshold"] = gap_up_threshold
            
        return filtered_tickers

    def filter_stocks_for_backtesting(self, all_tickers):
        filtered_tickers = self.daily_performance_filter_for_backtesting(all_tickers)
        filtered_tickers = self.relative_volume_filter_for_backtesting(filtered_tickers)
        if config.enable_gap_up_filter:
            filtered_tickers = self.gap_up_filter_for_backtesting(filtered_tickers)

        logging.info("New Day: %s. Trading on %s stocks: %s", self.data_handler.get_latest_data(all_tickers[0])[0].datetime, len(filtered_tickers.keys()), list(filtered_tickers.keys()))
        for ticker, values in filtered_tickers.items():
            logging.info(f"{ticker}: {values}")

        return list(filtered_tickers.keys())
