import math
from events.signal_event import SignalEvent
from strategies_old.abstract_strategy import AbstractStrategy

class BuyAndHoldStrategy(AbstractStrategy):
    """
        This is an extremely simple strategy that goes LONG all of the
        symbols as soon as a bar is received. It will never exit a position.

        It is primarily used as a testing mechanism for the Strategy class
        as well as a benchmark upon which to compare other strategies.
        """
    def __init__(self, data, events, portfolio):
        """
                Initialises the buy and hold strategy.

                Parameters:
                bars - The DataHandler object that provides bar information
                events - The Event Queue object.
                """
        self.data = data
        self.symbol_list = self.data.symbol_list
        self.events = events
        self.portfolio = portfolio
        self.name = 'Buy and Hold'

        self.bought = self._calculate_initial_bought()
        self.count = 1

    def _calculate_initial_bought(self):
        bought = {}
        for symbol in self.symbol_list:
            bought[symbol] = False

        return bought

    def calculate_signals(self, event):
        if event.type == 'MARKET':
            for symbol in self.symbol_list:
                data = self.data.get_latest_data(symbol, N=1)
                print(
                    f"Bar - Symbol: {symbol}, Time: {data[0].datetime}, High: {data[0].high}, Low: {data[0].low}, Close: {data[0].close}")
                if data is not None and len(data) > 0:
                    if self.bought[symbol] == False:
                        #quantity = math.floor((self.portfolio.current_holdings['cash'] * 0.5) / data[0].close)
                        quantity = 5
                        signal = SignalEvent(symbol, data[0].datetime, 'LONG', quantity)
                        print(
                            f"Raising BUY Signal - Symbol: {symbol}, Time: {data[0].datetime}, High: {data[0].high}, Low: {data[0].low}, Close: {data[0].close}")
                        self.events.put(signal)
                        self.bought[symbol] = True

    def plot(self):
        pass

class SellAndHoldStrategy(AbstractStrategy):
    def __init__(self, data, events, portfolio):
        self.data = data
        self.symbol_list = self.data.symbol_list
        self.events = events
        self.portfolio = portfolio
        self.name = 'Sell and Hold'

        self.bought = self._calculate_initial_bought()

    def _calculate_initial_bought(self):
        bought = {}
        for symbol in self.symbol_list:
            bought[symbol] = False

        return bought

    def calculate_signals(self, event):
        if event.type == 'MARKET':
            for symbol in self.symbol_list:
                data = self.data.get_latest_data(symbol)
                if data is not None and len(data) > 0:
                    if self.bought[symbol] == False:
                        quantity = math.floor(self.portfolio.current_holdings['cash'] / data['close'])
                        signal = SignalEvent(symbol, data['time'], 'SHORT', quantity)
                        self.events.put(signal)
                        self.bought[symbol] = True