import pandas as pd
from events.signal_event import SignalEvent
from strategies_old.abstract_strategy import AbstractStrategy

class BullFlagStrategy(AbstractStrategy):

    def __init__(self, data, events, portfolio,
                 flagpole_min_len=5,
                 pullback_len=3,
                 tolerance=0.4,
                 volume_confirm=False,
                 volume_multiplier=1.5,
                 is_backtest=True,
                 ):

        self.data = data
        self.symbol_list = self.data.symbol_list
        self.symbol_list_active = self.data.symbol_list
        self.events = events
        self.portfolio = portfolio
        self.name = 'Bull Flag Pattern'
        self.flagpole_len = flagpole_min_len
        self.pullback_len = pullback_len
        self.total_window = flagpole_min_len + pullback_len + 1
        self.tolerance = tolerance
        self.volume_confirm = volume_confirm
        self.volume_multiplier = volume_multiplier
        self.buffer = []
        self.is_backtest = is_backtest


    def calculate_signals(self, event):
        if event.type == 'MARKET':
            for symbol in self.symbol_list_active:

                #Check if stop/loss or take/profit condition met
                #Different logic for backtesting and real-time testing


                #if realtime, get pattern bars from new DS
                #buffer = self.data.new_ds
                if self.is_backtest:
                    buffer = self.data.get_latest_data(symbol, N=self.total_window)

                else:
                    buffer = self.data.get_latest_data_aggregated(symbol, N=self.total_window)




                if len(buffer) < self.total_window:
                    continue
                else:
                    df = pd.DataFrame(buffer)
                    flagpole_df = df.iloc[:self.flagpole_len]
                    pullback_df = df.iloc[self.flagpole_len:-1]
                    latest_bar = df.iloc[-1]

                    uptrend = all (
                        flagpole_df['close'].iloc[i] > flagpole_df['close'].iloc[i-1]
                        for i in range(1, len(flagpole_df))
                    )

                    max_flag_close = flagpole_df['close'].max()
                    min_pullback_close = pullback_df['close'].min()
                    pullback_ok = min_pullback_close > (max_flag_close * (1 - self.tolerance))

                    breakout = latest_bar['close'] > pullback_df['high'].max()

                    if self.volume_confirm:
                        avg_volume = flagpole_df['volume'].mean()
                        breakout_volume = latest_bar['volume']
                        volume_ok = breakout_volume > (avg_volume * self.volume_multiplier)
                    else:
                        volume_ok = True

                    if uptrend and pullback_ok and breakout and volume_ok:
                        signal = SignalEvent(symbol, latest_bar[0].datetime, 'LONG', 2)
                        self.events.put(signal)



    def plot(self):
        pass
