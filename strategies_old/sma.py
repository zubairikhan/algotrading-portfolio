import logging
from ta import trend
from events.signal_event import SignalEvent
from helper import is_trading_cutoff_time
from strategies_old.abstract_strategy import AbstractStrategy
import pandas as pd

class SMAStrategy(AbstractStrategy):

    def __init__(self, data, events, portfolio, short_period, long_period, cutoff_time, take_profit_margin=0.5, is_backtest=True):
        
        self.data = data
        #self.symbol_list = self.data.symbol_list
        #self.symbol_list_active = self.data.symbol_list
        self.events = events
        self.portfolio = portfolio
        self.name = 'SMA Strategy'
        self.short_period = short_period
        self.long_period = long_period
        self.cutoff_time = cutoff_time
        self.bought = self._setup_initial_bought()
        self.exit_levels = self._setup_initial_exit_levels()
        self.is_backtest = is_backtest
        self.last_aggregated_bar = self._setup_last_aggregated_bar()
        self.take_profit_margin = take_profit_margin

        logging.info(f"Initialized SMA Strategy with Short Period: {self.short_period}, Long Period: {self.long_period}, Take Profit Margin: {self.take_profit_margin}%\n")

    def _setup_last_aggregated_bar(self):
        last_aggregated_bar = {}
        for symbol in self.data.symbol_list:
            last_aggregated_bar[symbol] = ''

        return last_aggregated_bar

    def _setup_initial_exit_levels(self):
        exit_levels = {}
        for symbol  in self.data.symbol_list:
            exit_levels[symbol] = {
                'stop_loss': 0,
                'take_profit': 0
            }
        return exit_levels

    def _setup_initial_bought(self):
        bought = {}
        for symbol in self.data.symbol_list:
            bought[symbol] = False

        return bought

    def on_order_filled(self, symbol, direction, fill_price):
        if direction == 'BUY':
            self._update_exit_levels(symbol, fill_price)

    def _update_exit_levels(self, symbol, buy_price):
        take_profit = buy_price + (self.take_profit_margin / 100) * buy_price
        self.exit_levels[symbol]['take_profit'] = take_profit
        logging.info(f"Exit levels Updated for {symbol}: New TakeProfit: {take_profit}\n")

    def calculate_sma(self, df):
        df['SMA_short'] = trend.sma_indicator(df['close'], window=self.short_period)
        df['SMA_long'] = trend.sma_indicator(df['close'], window=self.long_period)
        return df

    def calculate_signals(self, event):
        if event.type == 'MARKET':

            five_sec_bar = None
            for symbol in self.data.symbol_list_active:

                if self.is_backtest:
                    is_new_bar = True
                    data = self.data.get_latest_data(symbol, N=0)
                else:
                    is_new_bar = False
                    data = self.data.get_latest_data_aggregated(symbol, N=0)
                    if self.last_aggregated_bar[symbol] != data:
                        is_new_bar = True
                        self.last_aggregated_bar[symbol] = data

                    five_sec_bar = self.data.get_latest_data(symbol)[0]

                if five_sec_bar is not None:
                    logging.info(f'5-sec-bar - Time: {five_sec_bar.datetime}, Close: {five_sec_bar.close}')

                if len(data) == 0:
                    continue


                df = pd.DataFrame(data, columns=["symbol", "date", "open", "high", "low", "close", "volume"])
                df = df.drop(["symbol"], axis=1)
                df.set_index('date', inplace=True, drop=False)

                latest = df.iloc[-1]

                if is_trading_cutoff_time(latest['date']):
                    #if self.is_backtest:
                    #    data.ib_client.cancel

                    # sell all
                    quantity = self.portfolio.current_positions[symbol]
                    logging.info(f"Symbol: {symbol} - Market Closing -  Time: {latest['date']}, Latest close: {latest['close']}\n")
                    if quantity > 0:
                        self.bought[symbol] = False
                        logging.info(f"Raising Signal [{latest['date']}]: SELL (Market Closing) for {symbol}, Qty: {quantity}\n")
                        signal = SignalEvent(symbol, df.iloc[-1]['date'], 'SHORT', quantity)
                        self.events.put(signal)
                    continue


                if self.bought[symbol]:
                    signal = self.process_exit_strategy(symbol, self.data.get_latest_data(symbol)[0])
                    if signal is not None:
                        self.bought[symbol] = False
                        self.events.put(signal)
                        continue


                if data is not None and is_new_bar and len(data) >= self.long_period:
                    df = self.calculate_sma(df)
                    latest = df.iloc[-1]
                    exit_levels = self.exit_levels[symbol]
                    try:
                        value = (str(five_sec_bar.datetime), five_sec_bar.close)
                    except AttributeError:
                        value = None
                    logging.info(
                        f"Symbol: {symbol}, 5-Sec-Bar: [{value}] | Aggregated Bar - Time: {latest['date']}, Open:{latest['open']}, High: {latest['high']}, Low: {latest['low']}, Close: {latest['close']}, Volume: {latest['volume']} "
                        f"SMA_short: {latest['SMA_short']}, SMA_long: {latest['SMA_long']}, StopLoss: {exit_levels['stop_loss']}, TakeProfit: {exit_levels['take_profit']}")

                    if self.bought[symbol] == False and self.has_short_term_sma_crossed_above_long_term_sma(df):
                        quantity = 2
                        logging.info(f"Raising Signal [{latest['date']}]: BUY (Short-term SMA [{latest['SMA_short']}] crossed ABOVE Long-term SMA [{latest['SMA_long']}]) for {symbol}, Qty: {quantity}\n")
                        self.bought[symbol] = True
                        buy_price = latest['close']
                        if self.is_backtest:
                            self._update_exit_levels(symbol, buy_price)
                        signal = SignalEvent(symbol, latest['date'], 'LONG', quantity)
                        self.events.put(signal)

                    elif self.bought[symbol] == True and self.has_short_term_sma_crossed_below_long_term_sma(df):
                        quantity = self.portfolio.current_positions[symbol]
                        if quantity > 0:
                            logging.info(f"Raising Signal [{latest['date']}]: SELL (Short-term SMA [{latest['SMA_short']}] crossed BELOW Long-term SMA[{latest['SMA_long']}]) for {symbol}, Qty: {quantity}\n")
                            self.bought[symbol] = False
                            signal = SignalEvent(symbol, latest['date'], 'SHORT', quantity)
                            self.events.put(signal)

    def has_short_term_sma_crossed_above_long_term_sma(self, df):
        prev = df.iloc[-2]
        curr = df.iloc[-1]



        if prev['SMA_short'] < prev['SMA_long'] and curr['SMA_short'] > curr['SMA_long']:
            logging.info(
                f"SMA CROSSED ABOVE - PrevSMA_Short: {prev['SMA_short']}, PrevSMA_long: {prev['SMA_long']}, CurrSMA_short: {curr['SMA_short']}, CurrSMA_long: {curr['SMA_long']}")
            return True

        return False

    def has_short_term_sma_crossed_below_long_term_sma(self, df):
        prev = df.iloc[-2]
        curr = df.iloc[-1]

        if prev['SMA_short'] > prev['SMA_long'] and curr['SMA_short'] < curr['SMA_long']:
            logging.info(
                f"SMA CROSSED BELOW - PrevSMA_Short: {prev['SMA_short']}, PrevSMA_long: {prev['SMA_long']}, CurrSMA_short: {curr['SMA_short']}, CurrSMA_long: {curr['SMA_long']}")
            return True

        return False


    def process_exit_strategy(self, symbol, bar):
        #TODO: refactor
        quantity = self.portfolio.current_positions[symbol]
        exit_levels = self.exit_levels[symbol]
        if self.is_backtest:
            if bar.high > exit_levels['take_profit']:
                logging.info(
                    f"Symbol: {symbol}, Time: {bar.datetime}, High: {bar.high}, Low: {bar.low}, Close: {bar.close}, "
                    f"SMA_short: [Uncomputed], SMA_long: [Uncomputed], StopLoss: {exit_levels['stop_loss']}, TakeProfit: {exit_levels['take_profit']}")
                logging.info(
                    f"Raising Signal [{bar.datetime}]: SELL (high {bar.high} crossed take_profit {exit_levels['take_profit']}) for {symbol}, Qty: {quantity}\n")
                return SignalEvent(symbol, bar.datetime, 'SHORT', quantity, price=exit_levels['take_profit'])
        else:
            if bar.close > exit_levels['take_profit']:
                logging.info(
                    f"Symbol: {symbol}, Time: {bar.datetime}, High: {bar.high}, Low: {bar.low}, Close: {bar.close}, "
                    f"SMA_short: [Uncomputed], SMA_long: [Uncomputed], StopLoss: {exit_levels['stop_loss']}, TakeProfit: {exit_levels['take_profit']}")
                logging.info(
                    f"Raising Signal [{bar.datetime}]: SELL (close {bar.close} crossed take_profit {exit_levels['take_profit']}) for {symbol}, Qty: {quantity}\n")
                return SignalEvent(symbol, bar.datetime, 'SHORT', quantity)

        return None

    def plot(self):
        pass
