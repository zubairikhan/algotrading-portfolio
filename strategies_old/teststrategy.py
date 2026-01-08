from events.signal_event import SignalEvent
from strategies_old.abstract_strategy import AbstractStrategy

class TestStrategy(AbstractStrategy):

    def __init__(self, data, events, portfolio):
        
        self.data = data
        self.symbol_list = self.data.symbol_list
        self.events = events
        self.portfolio = portfolio
        self.name = 'Test Strategy'
        self.bought, self.sold = self._calculate_initial_bought_and_sold()

        

    def _calculate_initial_bought_and_sold(self):
        bought = {}
        sold = {}
        for symbol in self.symbol_list:
            bought[symbol] = False
            sold[symbol] = False 
        
        return bought, sold
   
    def calculate_signals(self, event):
        if event.type == 'MARKET':
            for symbol in self.symbol_list:
                data = self.data.get_latest_data(symbol, N=1)
                if data is not None and len(data) > 0:
                    if self.bought[symbol] == False:
                        quantity = 2
                        signal = SignalEvent(symbol, data[0].datetime, 'LONG', quantity)
                        print(f"Strategy: Buy Signal Raised for {symbol}, Qty: {quantity}")
                        self.events.put(signal)
                        self.bought[symbol] = True
                    
                    elif self.bought[symbol] == True and self.sold[symbol] == False:
                        quantity = self.portfolio.current_positions[symbol]
                        if quantity > 0:
                            signal = SignalEvent(symbol, data[0].datetime, 'SHORT', quantity)
                            print(f"Strategy: Sell Signal Raised for {symbol}, Qty: {quantity}")
                            self.events.put(signal)
                            self.sold[symbol] = True

    def plot(self):
        pass
