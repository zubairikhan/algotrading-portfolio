import pandas as pd
import matplotlib.pyplot as plt
import math
from matplotlib import style
from events.signal_event import SignalEvent
from strategies_old.abstract_strategy import AbstractStrategy

class MovingAveragesLongStrategy(AbstractStrategy):
    def __init__(self, data, events, portfolio, short_period, long_period, verbose=False, version=1):
        self.data = data
        self.symbol_list = self.data.symbol_list
        self.events = events
        self.portfolio = portfolio
        self.short_period = short_period
        self.long_period = long_period
        self.name = 'Moving Averages Long'
        self.verbose = verbose
        self.version = version

        self.signals = self._setup_signals()
        self.strategy = self._setup_strategy()
        self.bought = self._setup_initial_bought()

    def _setup_signals(self):
        #this function creates the dataframe which stores the date/time and the signal that is generated at that timer
        signals = {}
        for symbol in self.symbol_list:
            signals[symbol] = pd.DataFrame(columns=['Date', 'Signal'])

        return signals

    def _setup_strategy(self):
        strategy = {}
        for symbol in self.symbol_list:
            strategy[symbol] = pd.DataFrame(columns=['Date', 'Short', 'Long'])

        return strategy

    def _setup_initial_bought(self):
        bought = {}
        for symbol in self.symbol_list:
            bought[symbol] = False

        return bought

    def calculate_long_short(self, df):
        price_short = None
        price_long = None
        if self.version == 1:
            price_short = df['Close'].ewm(span=self.short_period, min_periods=self.short_period, adjust=False).mean()[-1]
            price_long = df['Close'].ewm(span=self.long_period, min_periods=self.long_period, adjust=False).mean()[-1]
        else:
            price_short = df['Close'].tail(self.long_period).ewm(span=self.short_period, adjust=False).mean()[-1]
            price_long = df['Close'].tail(self.long_period).ewm(span=self.long_period, adjust=False).mean()[-1]

        return price_short, price_long

    def calculate_signals(self, event):
        if event.type == 'MARKET':
            for symbol in self.symbol_list:
                data = self.data.get_latest_data(symbol, N=-1)
                df = pd.DataFrame(data, columns=['Symbol','Date','Close'])
                df = df.drop(['Symbol'], axis=1)
                df.set_index('Date', inplace=True)
                if data is not None and len(data) >= self.long_period:
                    price_short, price_long = self.calculate_long_short(df)
                    date = df.index.values[-1]
                    price = df['Close'][-1]
                    self.strategy[symbol] = self.strategy[symbol].append({'Date': date, 'Short': price_short, 'Long': price_long}, ignore_index=True)
                    if self.bought[symbol] == False and price_short > price_long:
                        quantity = math.floor(self.portfolio.current_holdings['cash'] / (price * len(self.symbol_list)))
                        signal = SignalEvent(symbol, date, 'LONG', quantity)
                        self.events.put(signal)
                        self.bought[symbol] = True
                        self.signals[symbol] = self.signals[symbol].append({'Signal': quantity, 'Date': date}, ignore_index=True)
                        if self.verbose: print("Long", date, price)
                    elif self.bought[symbol] == True and price_short < price_long:
                        quantity = self.portfolio.current_positions[symbol]
                        signal = SignalEvent(symbol, date, 'EXIT', quantity)
                        self.events.put(signal)
                        self.bought[symbol] = False
                        self.signals[symbol] = self.signals[symbol].append({'Signal': -quantity, 'Date': date}, ignore_index=True)
                        if self.verbose: print("Exit", date, price)

    def plot(self):
        style.use('ggplot')

        for symbol in self.symbol_list:
            self.strategy[symbol].set_index('Date', inplace=True)
            self.signals[symbol].set_index('Date', inplace=True)
            signals = self.signals[symbol]
            strategy_fig, strategy_ax = plt.subplots()
            df = self.data.all_data[symbol].copy()
            df.columns = [symbol]
            # df['Short'] = df['OMXS30'].ewm(span=self.short_period, min_periods=self.short_period, adjust=False).mean()
            # df['Long'] = df['OMXS30'].ewm(span=self.long_period, min_periods=self.long_period, adjust=False).mean()

            df.plot(ax=strategy_ax, color='dodgerblue', linewidth=1.0)

            df['Short'] = df[symbol].ewm(span=self.short_period, min_periods=self.short_period, adjust=False).mean()
            df['Long'] = df[symbol].ewm(span=self.long_period, min_periods=self.long_period, adjust=False).mean()

            short_index = signals[signals['Signal'] < 0].index
            long_index = signals[signals['Signal'] > 0].index

            strategy_ax.plot(self.strategy[symbol]['Short'], label='Short EMA', color='grey')
            strategy_ax.plot(self.strategy[symbol]['Long'], label='Long EMA', color='k')
            strategy_ax.plot(short_index, df[symbol].loc[short_index], 'v', markersize=10, color='r', label='Exit')
            strategy_ax.plot(long_index, df[symbol].loc[long_index], '^', markersize=10, color='g', label='Long')

            strategy_ax.set_title(self.name)
            strategy_ax.set_xlabel('Time')
            strategy_ax.set_ylabel('Value')
            strategy_ax.legend()

        plt.show()

class MovingAveragesLongShortStrategy(AbstractStrategy):
    def __init__(self, data, events, portfolio, short_period, long_period, version=1):
        self.data = data
        self.symbol_list = self.data.symbol_list
        self.events = events
        self.portfolio = portfolio
        self.short_period = short_period
        self.long_period = long_period
        self.name = 'Moving Averages Long Short'
        self.version = version

        self.signals = self._setup_signals()
        self.strategy = self._setup_strategy()
        self.bought = self._setup_initial_bought()

    def _setup_signals(self):
        signals = {}
        for symbol in self.symbol_list:
            signals[symbol] = pd.DataFrame(columns=['Date', 'Signal'])

        return signals

    def _setup_strategy(self):
        strategy = {}
        for symbol in self.symbol_list:
            strategy[symbol] = pd.DataFrame(columns=['Date', 'Short', 'Long'])

        return strategy

    def _setup_initial_bought(self):
        bought = {}
        for symbol in self.symbol_list:
            bought[symbol] = False

        return bought

    def calculate_long_short(self, df):
        price_short = None
        price_long = None
        if self.version == 1:
            price_short = df['Close'].ewm(span=self.short_period, min_periods=self.short_period, adjust=False).mean()[-1]
            price_long = df['Close'].ewm(span=self.long_period, min_periods=self.long_period, adjust=False).mean()[-1]
        else:
            price_short = df['Close'].tail(self.long_period).ewm(span=self.short_period, adjust=False).mean()[-1]
            price_long = df['Close'].tail(self.long_period).ewm(span=self.long_period, adjust=False).mean()[-1]

        return price_short, price_long

    def calculate_signals(self, event):
        if event.type == 'MARKET':
            for symbol in self.symbol_list:
                data = self.data.get_latest_data(symbol, N=-1)
                df = pd.DataFrame(data, columns=['Symbol','Date','Close'])
                df = df.drop(['Symbol'], axis=1)
                df.set_index('Date', inplace=True)
                if data is not None and len(data) >= self.long_period:
                    price_short, price_long = self.calculate_long_short(df)
                    date = data[-1][self.data.time_col]
                    price = data[-1][self.data.price_col]
                    if self.bought[symbol] == False and price_short > price_long:
                        current_positions = self.portfolio.current_positions[symbol]
                        quantity = math.floor(self.portfolio.current_holdings['cash'] / price + current_positions)
                        signal = SignalEvent(symbol, date, 'EXIT', math.fabs(current_positions))
                        self.events.put(signal)
                        signal = SignalEvent(symbol, date, 'LONG', quantity)
                        self.events.put(signal)
                        self.bought[symbol] = True
                        self.signals[symbol] = self.signals[symbol].append({'Signal': quantity, 'Date': date}, ignore_index=True)
                        if self.verbose: print("Long", date, price)
                    elif self.bought[symbol] == True and price_short < price_long:
                        quantity = self.portfolio.current_positions[symbol]
                        signal = SignalEvent(symbol, date, 'EXIT', quantity)
                        self.events.put(signal)
                        signal = SignalEvent(symbol, date, 'SHORT', quantity)
                        self.events.put(signal)
                        self.bought[symbol] = False
                        self.signals[symbol] = self.signals[symbol].append({'Signal': -quantity, 'Date': date}, ignore_index=True)
                        if self.verbose: print("Short", date, price)

    def plot(self):
        style.use('ggplot')

        for symbol in self.symbol_list:
            self.strategy[symbol].set_index('Date', inplace=True)
            self.signals[symbol].set_index('Date', inplace=True)
            signals = self.signals[symbol]
            strategy_fig, strategy_ax = plt.subplots()
            df = self.data.all_data[symbol].copy()
            df.columns = ['OMXS30']
            # df['Short'] = df['OMXS30'].ewm(span=self.short_period, min_periods=self.short_period, adjust=False).mean()
            # df['Long'] = df['OMXS30'].ewm(span=self.long_period, min_periods=self.long_period, adjust=False).mean()

            df.plot(ax=strategy_ax, color='dodgerblue', linewidth=1.0)

            short_index = signals[signals['Signal'] < 0].index
            long_index = signals[signals['Signal'] > 0].index

            strategy_ax.plot(self.strategy[symbol]['Short'], label='Short EMA', color='grey')
            strategy_ax.plot(self.strategy[symbol]['Long'], label='Long EMA', color='k')
            strategy_ax.plot(short_index, df['OMXS30'].loc[short_index], 'v', markersize=10, color='r', label='Short')
            strategy_ax.plot(long_index, df['OMXS30'].loc[long_index], '^', markersize=10, color='g', label='Long')

            strategy_ax.set_title(self.name)
            strategy_ax.set_xlabel('Time')
            strategy_ax.set_ylabel('Value')
            strategy_ax.legend()

        plt.show()

class MovingAveragesMomentumStrategy(AbstractStrategy):
    def __init__(self, data, events, portfolio, short_period, long_period):
        self.data = data
        self.symbol_list = self.data.symbol_list
        self.events = events
        self.portfolio = portfolio
        self.short_period = short_period
        self.long_period = long_period
        self.name = 'Moving Averages Momentum'

    def calculate_long_short(self, df):
        price_short = None
        price_long = None
        if self.version == 1:
            price_short = df['Close'].ewm(span=self.short_period, min_periods=self.short_period, adjust=False).mean()[-1]
            price_long = df['Close'].ewm(span=self.long_period, min_periods=self.long_period, adjust=False).mean()[-1]
        else:
            price_short = df['Close'].tail(self.long_period).ewm(span=self.short_period, adjust=False).mean()[-1]
            price_long = df['Close'].tail(self.long_period).ewm(span=self.long_period, adjust=False).mean()[-1]

        return price_short, price_long

    def calculate_signals(self, event):
        if event.type == 'MARKET':
            for symbol in self.symbol_list:
                data = self.data.get_latest_data(symbol, N=-1)
                df = pd.DataFrame(data, columns=['Symbol','Date','Close'])
                df = df.drop(['Symbol'], axis=1)
                df.set_index('Date', inplace=True)
                if data is not None and len(data) >= self.long_period:
                    price_short, price_long = self.calculate_long_short(df)
                    diff = price_long - price_short
                    factor = math.fabs(2*math.atan(diff) / math.pi)
                    date = data[-1][self.data.time_col]
                    price = data[-1][self.data.price_col]
                    if price_short >= price_long:
                        quantity = math.floor(factor * self.portfolio.current_holdings['cash'] / price)
                        if quantity != 0:
                            signal = SignalEvent(symbol, date, 'LONG', quantity)
                            self.events.put(signal)
                            if self.verbose: print('Long', date, price)
                    else:
                        quantity = math.floor(factor/2 * self.portfolio.current_positions[symbol])
                        if quantity != 0:
                            signal = SignalEvent(symbol, date, 'SHORT', quantity)
                            self.events.put(signal)
                            if self.verbose: print('Short', date, price)



class MovingAveragesLong15min(AbstractStrategy):
    def __init__(self, data, events, portfolio, short_period, long_period, breakout_period, verbose=False, version=1):
        self.data = data
        self.symbol_list = self.data.symbol_list
        self.events = events
        self.portfolio = portfolio
        self.short_period = short_period
        self.long_period = long_period
        self.breakout_period = breakout_period
        self.name = 'Moving Averages Long 15min'
        self.verbose = verbose
        self.version = version

        self.signals = self._setup_signals()
        self.strategy = self._setup_strategy()
        self.bought = self._setup_initial_bought()
        self.breakout_price = self._setup_breakout_price()

    def _setup_signals(self):
        # this function creates the dataframe which stores the date/time and the signal that is generated at that timer
        signals = {}
        for symbol in self.symbol_list:
            signals[symbol] = pd.DataFrame(columns=['Date', 'Signal'])

        return signals

    def _setup_breakout_price(self):
        # this function creates the dictionary which stores the breakout price for all symbols
        breakout_price = {}
        for symbol in self.symbol_list:
            breakout_price[symbol] = 0

        return breakout_price

    def _setup_strategy(self):
        strategy = {}
        for symbol in self.symbol_list:
            strategy[symbol] = pd.DataFrame(columns=['Date', 'Short', 'Long'])

        return strategy

    def _setup_initial_bought(self):
        bought = {}
        for symbol in self.symbol_list:
            bought[symbol] = False

        return bought


    def calculate_long_short(self, df):
        price_short = None
        price_long = None
        if self.version == 1:
            price_short = df['Close'].ewm(span=self.short_period, min_periods=self.short_period, adjust=False).mean()[
                -1]
            price_long = df['Close'].ewm(span=self.long_period, min_periods=self.long_period, adjust=False).mean()[-1]
        else:
            price_short = df['Close'].tail(self.long_period).ewm(span=self.short_period, adjust=False).mean()[-1]
            price_long = df['Close'].tail(self.long_period).ewm(span=self.long_period, adjust=False).mean()[-1]

        return price_short, price_long



    def calculate_signals(self, event):
        if event.type == 'MARKET':
            for symbol in self.symbol_list:
                data = self.data.get_latest_data(symbol, N=-1)

                #calculate price range for breakout. Use first 30min i.e. max and min price
                if len(data) == self.breakout_period:
                    closing_prices = [x[2] for x in data]
                    self.breakout_price[symbol] = max(closing_prices)

                df = pd.DataFrame(data, columns=['Symbol', 'Date', 'Close'])
                df = df.drop(['Symbol'], axis=1)
                df.set_index('Date', inplace=True)
                if data is not None and len(data) >= self.long_period:
                    price_short, price_long = self.calculate_long_short(df)
                    date = df.index.values[-1]
                    price = df['Close'][-1]
                    self.strategy[symbol] = self.strategy[symbol].append(
                        {'Date': date, 'Short': price_short, 'Long': price_long}, ignore_index=True)
                    # If short ema price is higher than long ema price and more than 5min are left before day ends and breakout_price is exceeded
                    if (self.bought[symbol] == False and price_short > price_long) and (pd.Timestamp(date).hour <= 21 and pd.Timestamp(date).minute < 55) and price > self.breakout_price[symbol]:
                            quantity = math.floor(self.portfolio.current_holdings['cash'] / (price * len(self.symbol_list)))
                            signal = SignalEvent(symbol, date, 'LONG', quantity)
                            self.events.put(signal)
                            self.bought[symbol] = True
                            self.signals[symbol] = self.signals[symbol].append({'Signal': quantity, 'Date': date},ignore_index=True)
                            if self.verbose: print("Long", date, price)
                    # If short ema price is lower than long ema price or time is 5min or less before day ends
                    elif (self.bought[symbol] == True and price_short < price_long) or (pd.Timestamp(date).hour >= 21 and pd.Timestamp(date).minute >= 55):
                        quantity = self.portfolio.current_positions[symbol]
                        signal = SignalEvent(symbol, date, 'EXIT', quantity)
                        self.events.put(signal)
                        self.bought[symbol] = False
                        self.signals[symbol] = self.signals[symbol].append({'Signal': -quantity, 'Date': date},ignore_index=True)
                        if self.verbose: print("Exit", date, price)




    def plot(self):
        style.use('ggplot')

        for symbol in self.symbol_list:
            self.strategy[symbol].set_index('Date', inplace=True)
            self.signals[symbol].set_index('Date', inplace=True)
            signals = self.signals[symbol]
            #strategy_fig, strategy_ax = plt.subplots()
            df = self.data.all_data[symbol].copy()
            df.columns = [symbol]
            #df.plot(ax=strategy_ax, color='dodgerblue', linewidth=1.0)

            df['Short'] = df[symbol].ewm(span=self.short_period, min_periods=self.short_period, adjust=False).mean()
            df['Long'] = df[symbol].ewm(span=self.long_period, min_periods=self.long_period, adjust=False).mean()


            short_index = signals[signals['Signal'] < 0].index
            long_index = signals[signals['Signal'] > 0].index

            strategy_fig, strategy_ax = plt.subplots()
            strategy_ax.plot(df['Long'], label='Long EMA', color='k')
            strategy_ax.plot(df['Short'], label='Short EMA', color='grey')
            strategy_ax.plot(short_index, df[symbol].loc[short_index], 'v', markersize=10, color='r', label='Exit')
            strategy_ax.plot(long_index, df[symbol].loc[long_index], '^', markersize=10, color='g', label='Long')
            strategy_ax.plot(df[symbol], label=symbol, color='blue')
            strategy_ax.set_title(self.name)
            strategy_ax.set_xlabel('Time')
            strategy_ax.set_ylabel('Value')
            strategy_ax.legend()

        plt.show()
