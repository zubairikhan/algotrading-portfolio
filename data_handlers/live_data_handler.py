import pandas as pd
import queue

from database_repository import DatabaseRepository
import helper
from ibapi.common import RealTimeBar

from bar_aggregator import BarAggregator
from data_handlers.data_handler import DataHandler
from data_handlers.types.bar import Bar
from events.market_event import MarketEvent
from datetime import datetime
from ibapi.contract import Contract

from ib_client import IBClient
import logging


class LiveDataHandler(DataHandler):
    def __init__(self, events, symbol_list, database_repository, bar_granularity):
        self.events = events
        self.symbol_list = symbol_list
        self.symbol_list_active = symbol_list
        self.ib_client = IBClient('127.0.0.1', 7497, 4)
        self.fundamental_data = {}
        self.bar_granularity = bar_granularity

        self.symbol_data = {}
        self.symbol_dataframe = {}
        self.latest_symbol_data = {}
        self.latest_symbol_data_aggregated = {}
        self.aggregated_symbol_records = {}
        self.all_data = {}

        self.continue_backtest = True

        self.database_repository: DatabaseRepository = database_repository

        self.filter_data_size = 0
        self.volume_filter_data_complete = 0
        self.missing_first_bar = []
        self.bars = []


        self.bar_aggregators = {}
        for symbol in self.symbol_list:
            self.all_data[symbol] = None
            self.latest_symbol_data[symbol] = []
            self.latest_symbol_data_aggregated[symbol] = []
            self.symbol_data[symbol] = queue.Queue()
            self.bar_aggregators[symbol] = BarAggregator(symbol, self.store_aggregated_bar, source_granularity=5, target_granularity=self.bar_granularity) #for incoming 5sec bar


    def capture_historical_data(self, bar, req_id):
        # t = datetime.fromtimestamp(int(bar.date))

        dt = helper.string_to_datetime(bar.date)

        data = {
            'date': dt,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': int(bar.volume)
        }

        self.bars.append(data)

    def is_volume_data_complete(self, needed):
        return self.volume_filter_data_complete >= needed

    def track_missing_first_bar(self, reqId):
        symbol = self.symbol_list_active[reqId]
        self.missing_first_bar.append(symbol)
        self.increment_volume_filter_data()


    def increment_volume_filter_data(self):
        self.volume_filter_data_complete += 1

    def historical_data_end(self, req_id):
        symbol = self.symbol_list_active[req_id]
        for bar in self.bars:
            self.store_aggregated_bar(symbol, bar)
        self.increment_volume_filter_data()
        self.bars = []

    def fetch_first_bar_of_day(self, granularity):

        bar_size = f'{int(granularity/60)} mins'
        duration = f'{granularity-5} S'
        for req_id, symbol in enumerate(self.symbol_list_active):
            contract = Contract()
            contract.symbol = symbol
            contract.secType = 'STK'
            contract.exchange = 'SMART'
            contract.currency = 'USD'
            what_to_show = 'TRADES'

            
            self.ib_client.reqHistoricalData(
                req_id, contract, 
                endDateTime='', durationStr=duration, barSizeSetting=bar_size,
                whatToShow=what_to_show, useRTH=True, formatDate=1, keepUpToDate=False, chartOptions=[]
            )



    def fetch_historical_ohlcv_data(self, start_time, end_time):
        symbol_records = {}
        rows = self.database_repository.get_stock_data(start_time, end_time, self.symbol_list)
        for row in rows:
            bar = list(row)
            bar[1] = datetime.strptime(bar[1], "%Y-%m-%d %H:%M:%S")
            stock_symbol = bar[0]
            if stock_symbol not in symbol_records:
                symbol_records[stock_symbol] = []
            symbol_records[stock_symbol].append(bar[1:])

        symbol_records = self.filter_out_stocks_with_missing_records(symbol_records, start_time, end_time)
        if (symbol_records is None) or (len(symbol_records) == 0):
            raise ValueError("No symbols with complete data found in the given time range.")
        
        symbol_records = self.aggregate_bars(symbol_records, self.bar_granularity)

        self.symbol_list = list(symbol_records.keys())
        self.symbol_list_active = self.symbol_list

        for symbol, records in symbol_records.items():
            for record in records:
                self.store_aggregated_bar(symbol, record)
        #self.fetch_float_data()
        self.filter_data_size = len(self.latest_symbol_data_aggregated[self.symbol_list[0]])

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



    @staticmethod
    def filter_out_stocks_with_missing_records(symbol_records, start_time, end_time):

        start_date = start_time.strftime("%Y-%m-%d")
        end_date = end_time.strftime("%Y-%m-%d")

        trading_days = helper.get_trading_days(start_date, end_date)
        trading_days = trading_days[:-1]
        expected_bars_per_day = len(helper.expected_times_for_day(trading_days[0])) - 1

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
            df = df[
                df['date'].isin(trading_days) &
                (df['time'] >= market_open_time) &
                (df['time'] <= market_close_time)
            ]

            # Count bars per trading day
            daily_counts = df.groupby('date')['timestamp'].nunique()

            # Check for completeness
            if any(daily_counts.get(date, 0) != expected_bars_per_day for date in trading_days):
                symbols_to_delete.append(symbol)

        # Drop symbols with incomplete data
        for symbol in symbols_to_delete:
            logging.info("Missing data for symbol: %s for time period %s and %s", symbol, start_date, end_date)
            del symbol_records[symbol]

        logging.info(f"✅ Filtered dictionary now contains only complete symbols - Count({len(symbol_records.keys())}):")
        logging.info(list(symbol_records.keys()))
        return symbol_records


    def fetch_live_data(self, req_id):

        for symbol in self.symbol_list:


            contract = Contract()
            contract.symbol = symbol
            contract.secType = 'STK'
            contract.exchange = 'SMART'
            contract.currency = 'USD'
            what_to_show = 'TRADES'

            self.ib_client.reqRealTimeBars(req_id, contract, 5, what_to_show , False, [])
            req_id += 1
            

            #time.sleep(1)

    def convert_date_to_ib_format(self, end_date):
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        end_date = end_date_obj.strftime('%Y%m%d %H:%M:%S')
        return end_date

    def store_aggregated_bar(self, symbol, bar):
        bar = Bar(symbol, bar["date"], bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"])
        self.latest_symbol_data_aggregated[symbol].append(bar)


    def capture_live_data(self, req_id, bar:RealTimeBar):

        symbol = self.symbol_list[req_id]
        self.bar_aggregators[symbol].process_bar_for_aggregation(bar)

        bar.time = datetime.fromtimestamp(bar.time)

        data = {
            'date': bar.time,
            'open': bar.open_,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': int(bar.volume)
        }

        new_bar = pd.DataFrame([data])
        if symbol not in self.symbol_dataframe:
            self.symbol_dataframe[symbol] = new_bar
        else:
            self.symbol_dataframe[symbol] = pd.concat([self.symbol_dataframe[symbol], new_bar], ignore_index=True)

        self.all_data[symbol] = self.symbol_dataframe[symbol].copy()
        self.symbol_data[symbol].put(data)




    def get_latest_data(self, symbol, N=1):
        # This function gets the latest data for the symbol being considered, for the purppse of fill calculations or
        try:
            return self.latest_symbol_data[symbol][-N:]
        except KeyError:
            print("{symbol} is not a valid symbol.").format(symbol=symbol)

    def get_latest_data_aggregated(self, symbol, N=1):
        #This function gets the latest data for the symbol being considered, for the purppse of fill calculations or
        try:
            return self.latest_symbol_data_aggregated[symbol][-N:]
        except KeyError:
            print("{symbol} is not a valid symbol.").format(symbol=symbol)

    def _get_new_data(self, symbol):
        """
                Returns the latest bar from the data feed as a tuple of
                (sybmbol, datetime, open, low, high, close, volume).
                """
        data = self.symbol_data[symbol].get(block=True, timeout=6)
        return Bar(symbol, data["date"], data["open"], data["high"], data["low"], data["close"], data["volume"])

    def update_latest_data(self):
        # This function updates the data feed and creates a market event
        for symbol in self.symbol_list:
            data = None
            try:
                data = self._get_new_data(symbol)
            except queue.Empty:
                self.continue_backtest = False
            if data is not None:
                self.latest_symbol_data[symbol].append(data)
                # test123

        self.events.put(MarketEvent())

    def create_baseline_dataframe(self):
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

    def handle_termination(self, sig=None, frame=None):
        self.continue_backtest = False
        self.handle_ib_client()
        #sys.exit(0)

    def handle_ib_client(self):
        if not helper.IS_BACKTEST:
            print("Handling IB CLIENT")
            self.cancel_ib_data_subscription()
            #time.sleep(100)
            self.ib_client.disconnect()

    def cancel_ib_data_subscription(self):
        for idx, _ in enumerate(self.symbol_list):
            self.ib_client.cancelRealTimeBars(idx)
