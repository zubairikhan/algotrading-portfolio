import pandas as pd

import config
from data_handlers.types.bar import Bar
from database_repository import DatabaseRepository
import helper
from ibapi.common import RealTimeBar

from bar_aggregator import BarAggregator
from events.market_event import MarketEvent
from datetime import datetime, timedelta

import logging

class HistoricDBDataHandler(object):
    def __init__(self, events, symbol_list, database_repository, bar_granularity=300):
        """
                Initialises the historic data handler by requesting
                sql db defined by the engine_name. of each symbol in the symbol list
                Parameters:
                events - The Event Queue.
                sql_dir - Absolute directory path to the sql files.
                symbol_list - A list of symbol strings.
                engine_name = name of the sql db including data for symbols in the symbol_list

                """
        self.events = events
        self.symbol_list = symbol_list
        self.symbol_list_active = symbol_list
        self.symbol_data = {}
        self.symbol_dataframe = {}
        self.latest_symbol_data = {}
        self.all_data = {}

        self.bar_granularity = bar_granularity
        self.continue_backtest = True
        self.database_repository: DatabaseRepository = database_repository
        self.fundamental_data = {}
        self.aggregated_symbol_records = {}
        self.time_col = 1
        self.price_col = 2
        self.dst_date_change_start = datetime.strptime(config.dst_date_change_start, "%Y-%m-%d")
        self.dst_date_change_end = datetime.strptime(config.dst_date_change_end, "%Y-%m-%d")
        self.full_trading_days = []


    def fetch_historical_ohlcv_data(self, start_time, end_time):
        symbol_records = {}
        self.full_trading_days = helper.get_full_trading_days(start_time, end_time)
        rows = self.database_repository.get_stock_data(start_time, end_time, self.symbol_list)
        for row in rows:
            bar = list(row)
            date = datetime.strptime(bar[1], "%Y-%m-%d %H:%M:%S")

            # Skip day from backtesting if not a full trading day (exclude early market closes)
            if date.date() not in self.full_trading_days:
                continue

            date = self.adjust_for_dst(date)
            bar[1] = date
            stock_symbol = bar[0]
            if stock_symbol not in symbol_records:
                symbol_records[stock_symbol] = []
            symbol_records[stock_symbol].append(bar[1:])

        symbol_records = self.filter_out_stocks_with_missing_records(symbol_records, start_time, end_time)
        symbol_records = self.aggregate_bars(symbol_records, self.bar_granularity)

        self.symbol_list = list(symbol_records.keys())
        self.symbol_list_active = self.symbol_list

        self.symbol_dataframe = {k: pd.DataFrame(v, columns=['date', 'open', 'high', 'low','close','volume']) for k, v in symbol_records.items()}
        for symbol in self.symbol_dataframe.keys():
            self.all_data[symbol] = self.symbol_dataframe[symbol].copy()
            self.symbol_data[symbol] = self.symbol_dataframe[symbol].iterrows()
            self.latest_symbol_data[symbol] = []

    def adjust_for_dst(self, date):
        if date.date() <= self.dst_date_change_end.date() and date.date() >= self.dst_date_change_start.date():
            date = date + timedelta(hours=1)
        return date

    def fetch_float_data(self):
        rows = self.database_repository.get_stock_float(self.symbol_list)

        for row in rows:
            self.fundamental_data[row[0]] = {"float": row[1]}

    def on_aggregate_completion(self, symbol, completed_bar):
        if symbol not in self.aggregated_symbol_records:
            self.aggregated_symbol_records[symbol] = []
        self.aggregated_symbol_records[symbol].append(completed_bar)


    def aggregate_bars(self, symbol_records, bar_granularity):
        for symbol, records in symbol_records.items():
            bar_aggregator = BarAggregator(symbol, self.on_aggregate_completion, source_granularity=300, target_granularity=bar_granularity) #for incoming 5min(300s) bar

            for record in records:
                bar = RealTimeBar(time=record[0].timestamp(), open_=record[1], high=record[2], low=record[3], close=record[4], volume=record[5])
                bar_aggregator.process_bar_for_aggregation(bar)
            bar_aggregator._finalize_aggregated_bar()
        return self.aggregated_symbol_records

    def filter_out_stocks_with_missing_records(self, symbol_records, start_time, end_time):

        start_date = start_time.strftime("%Y-%m-%d")
        end_date = end_time.strftime("%Y-%m-%d")

        #expected_bars_per_day = len(helper.expected_times_for_day(trading_days_without_early_closes[0])) - 1
        expected_bars_per_day = helper.get_expected_number_of_bars_per_day(bar_granularity_in_seconds=300) # 5-minute bars stored in database

        symbols_to_delete = []

        market_open_time = datetime.strptime(helper.MKT_OPEN_TIME, '%H:%M:%S').time()
        market_close_time = datetime.strptime(helper.MKT_CLOSE_TIME, '%H:%M:%S').time()

        for symbol, records in symbol_records.items():
            if not records:
                symbols_to_delete.append(symbol)
                continue

            df = pd.DataFrame(records, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['date'] = df['timestamp'].dt.date
            df['time'] = df['timestamp'].dt.time

            # Filter to market hours only
            # and only full trading days
            df = df[
                df['date'].isin(self.full_trading_days) &
                (df['time'] >= market_open_time) &
                (df['time'] <= market_close_time)
            ]

            # Count bars per trading day
            daily_counts = df.groupby('date')['timestamp'].nunique()

            # Check for completeness
            if any(daily_counts.get(date, 0) != expected_bars_per_day for date in self.full_trading_days):
                symbols_to_delete.append(symbol)

        # Drop symbols with incomplete data
        for symbol in symbols_to_delete:
            logging.info("Missing data for symbol: %s for time period %s and %s", symbol, start_date, end_date)
            del symbol_records[symbol]

        logging.info(f"✅ Filtered dictionary now contains only complete symbols - Count({len(symbol_records.keys())}):")
        logging.info(list(symbol_records.keys()))
        return symbol_records


    def _get_new_data(self, symbol):
        """
                Returns the latest bar from the data feed as a tuple of
                (sybmbol, datetime, open, low, high, close, volume).
                """

        for _, row in self.symbol_data[symbol]:
            yield Bar(symbol, row["date"], row["open"], row["high"], row["low"], row["close"], row["volume"])

    def get_latest_data(self, symbol, N=1):
        # This function gets the latest data for the symbol being considered, for the purppse of fill calculations or
        try:
            return self.latest_symbol_data[symbol][-N:]
        except KeyError:
            print("{symbol} is not a valid symbol.".format(symbol=symbol))

    def update_latest_data(self):
        # This function updates the data feed and creates a market event
        for symbol in self.symbol_list:
            data = None
            try:
                data = next(self._get_new_data(symbol))
            except StopIteration:
                self.continue_backtest = False
            if data is not None:
                self.latest_symbol_data[symbol].append(data)
                # test123

        self.events.put(MarketEvent())
    def create_baseline_dataframe(self):

        #this creates a dataframe for a symbol and plots the percentage change in the symbol over the time period considered
        dataframe = None
        for symbol in self.symbol_list:
            df = self.symbol_dataframe[symbol]
            if dataframe is None:
                dataframe = pd.DataFrame(df['close'])
                dataframe.columns = [symbol]
            else:
                dataframe[symbol] = pd.DataFrame(df['close'])
            dataframe[symbol] = dataframe[symbol].pct_change()
            dataframe[symbol] = (1.0 + dataframe[symbol]).cumprod()

        return dataframe

    def handle_termination(self):
        pass