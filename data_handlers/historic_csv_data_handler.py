import pandas as pd
import os.path

from data_handlers.data_handler import DataHandler
from data_handlers.enums.data_format import DataFormat
from data_handlers.types.bar import Bar
import helper
from events.market_event import MarketEvent

class HistoricCSVDataHandler(DataHandler):
    """
        HistoricCSVDataHandler is designed to read CSV files for
        each requested symbol from disk and provide an interface
        to obtain the "latest" bar in a manner identical to a live
        trading interface.
        """
    def __init__(self, events, csv_dir, symbol_list, source=DataFormat.NASDAQ):
        """
                Initialises the historic data handler by requesting
                the location of the CSV files and a list of symbols.

                It will be assumed that all files are of the form
                'symbol.csv', where symbol is a string in the list.

                Parameters:
                events - The Event Queue.
                csv_dir - Absolute directory path to the CSV files.
                symbol_list - A list of symbol strings.
                """
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.symbol_dataframe = {}
        self.latest_symbol_data = {}
        self.all_data = {}
        self.continue_backtest = True

        self.time_col = 1
        self.price_col = 2

        self._open_convert_csv_files(source)

    def _open_convert_csv_files(self, source):
        """
                Opens the CSV files from the data directory, converting
                them into pandas DataFrames within a symbol dictionary.

                """
        combined_index = None
        for symbol in self.symbol_list:
            if source == DataFormat.NASDAQ:
                self.parse_nasdaq_csv(symbol)
            else:
                self.parse_yahoo_csv(symbol)

            if combined_index is None:
                combined_index = self.symbol_data[symbol].index
            else:
                combined_index.union(self.symbol_data[symbol].index)

            self.latest_symbol_data[symbol] = []

        #In case there are symbols with not the same time index due to missing dates etc, then reindexing shall be performed
        for symbol in self.symbol_list:
            self.symbol_dataframe[symbol] = self.symbol_data[symbol].reindex(index=combined_index, method='pad')
            self.all_data[symbol] = self.symbol_dataframe[symbol].copy()
            self.symbol_data[symbol] = self.symbol_dataframe[symbol].iterrows()

    #generator
    def _get_new_data(self, symbol):
        """
                Returns the latest bar from the data feed as a tuple of
                (sybmbol, datetime, open, low, high, close, volume).
                """
        for _, row in self.symbol_data[symbol]:
            yield Bar(symbol, row["date"], row["open"], row["high"], row["low"], row["close"], row["volume"])

    def get_latest_data(self, symbol, N=1):
        #This function gets the latest data for the symbol being considered, for the purppse of fill calculations or
        try:
            return self.latest_symbol_data[symbol][-N:]
        except KeyError:
            print("{symbol} is not a valid symbol.").format(symbol=symbol)

    def update_latest_data(self):
        #This function updates the data feed and creates a market event
        for symbol in self.symbol_list:
            data = None
            try:
                data = next(self._get_new_data(symbol))
            except StopIteration:
                self.continue_backtest = False
            if data is not None:
                self.latest_symbol_data[symbol].append(data)
                #test123

        self.events.put(MarketEvent())

    def create_baseline_dataframe(self):
        #this creates a dataframe for a symbol and plots the percentage change in the symbol over the time period considered
        dataframe = None
        for symbol in self.symbol_list:
            df = self.symbol_dataframe[symbol]
            if dataframe == None:
                dataframe = pd.DataFrame(df['close'])
                dataframe.columns = [symbol]
            else:
                dataframe[symbol] = pd.DataFrame(df['Close'])
            dataframe[symbol] = dataframe[symbol].pct_change()
            dataframe[symbol] = (1.0 + dataframe[symbol]).cumprod()

        return dataframe

    def parse_yahoo_csv(self, symbol):
        self.symbol_data[symbol] = pd.read_csv(os.path.join(self.csv_dir, symbol + '.csv'), header=0,index_col=0, parse_dates=True)

    def parse_nasdaq_csv(self, symbol):
        col_names = ['date','high', 'low','close','avg price','volume','turnover']
        tmp = pd.read_csv(os.path.join(self.csv_dir, symbol + '.csv'), parse_dates=True).iloc[:, :-1]
        tmp.columns = col_names
        tmp['date'] = tmp['date'].apply(helper.string_to_datetime)
        self.symbol_data[symbol] = tmp
        self.symbol_data[symbol]['open'] = tmp['close']
        #self.symbol_data[symbol] = pd.DataFrame(tmp['Closing price'], columns=['close'])
        #self.symbol_data[symbol].columns = ['Close']

        #self.symbol_data[symbol]['high'] = tmp['High price']
        #self.symbol_data[symbol]['low'] = tmp['Low price']
        #self.symbol_data[symbol]['Close'] = tmp['Closing price']
        # self.symbol_data[symbol]['Adj Close'] = tmp['Closing price']
        #self.symbol_data[symbol]['volume'] = tmp['Total volume']
        self.symbol_data[symbol] = self.symbol_data[symbol][self.symbol_data[symbol]['close'] > 0.0]