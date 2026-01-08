from data_handlers.data_handler import DataHandler
import time
import pandas as pd
import helper
from data_handlers.types.bar import Bar

from events.market_event import MarketEvent
from datetime import datetime
from ibapi.contract import Contract

from ib_client import IBClient


class IBDataHandler(DataHandler):
    def __init__(self, events, symbol_list):
        self.events = events
        self.symbol_list = symbol_list
        self.symbol_list_active = []
        self.ib_client = IBClient('127.0.0.1', 7497, 4)
        self.fundamental_data = {}

        self.symbol_data = {}
        self.symbol_dataframe = {}
        self.latest_symbol_data = {}
        self.all_data = {}
        self.continue_backtest = True

        self.time_col = 1
        self.price_col = 5

        self.bars = []
        self.symbol_generators = {}

    def fetch_historical_data(self, req_id, end_date, time_period, bar_size):
        end_date = self.convert_date_to_ib_format(end_date)
        for symbol in self.symbol_list:
            contract = Contract()
            contract.symbol = symbol
            contract.secType = 'STK'
            contract.exchange = 'SMART'
            contract.currency = 'USD'
            what_to_show = 'TRADES'

            self.ib_client.reqHistoricalData(
                req_id, contract, end_date, time_period, bar_size, what_to_show, True, 1, False, []
            )

            req_id += 1
            self.latest_symbol_data[symbol] = []

            time.sleep(1)

    def convert_date_to_ib_format(self, end_date):
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        end_date = end_date_obj.strftime('%Y%m%d %H:%M:%S')
        return end_date

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

    def historical_data_end(self, req_id):
        symbol = self.symbol_list[req_id]
        self.symbol_dataframe[symbol] = pd.DataFrame(self.bars)
        self.all_data[symbol] = self.symbol_dataframe[symbol].copy()
        self.symbol_data[symbol] = self.symbol_dataframe[symbol].iterrows()
        self.bars = []

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
            print("{symbol} is not a valid symbol.").format(symbol=symbol)

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