from abc import ABC, abstractmethod
import logging
import os
import shutil
import numpy as np
import pandas as pd
from events.signal_event import SignalEvent
import helper
from trade import Trade

class Strategy(ABC):
    def __init__(self, data_handler, events, portfolio, cutoff_time):
        self.data_handler = data_handler
        self.events = events
        self.portfolio = portfolio
        self.cutoff_time = cutoff_time
        self.name = 'Base Strategy'

        self.bought = self._setup_initial_bought()
        self.exit_levels = self._setup_initial_exit_levels()
        self.last_aggregated_bar = self._setup_last_aggregated_bar()
        self.trades = []
        self.active_trades = self._initialize_active_trades()


    @abstractmethod
    def calculate_signals(self, event):
        pass

    @abstractmethod
    def plot(self):
        pass

    @abstractmethod
    def post_data_fetch_setup(self):
        pass

    @abstractmethod
    def process_start_of_new_day(self):
        pass

    def fetch_latest_data(self, symbol, qty=0):
        if helper.IS_BACKTEST:
            is_new_bar = True
            data = self.data_handler.get_latest_data(symbol, N=qty) #get all data
            five_sec_bar = None
        else:
            is_new_bar = False
            data = self.data_handler.get_latest_data_aggregated(symbol, N=qty) # (-1 * self.data_handler.filter_data_size)) #get all aggregated data
            if self.last_aggregated_bar[symbol] is None or self.last_aggregated_bar[symbol].datetime != data[-1].datetime:
                is_new_bar = True
                self.last_aggregated_bar[symbol] = data[-1]

            five_sec_bar = self.data_handler.get_latest_data(symbol)[0]
        return five_sec_bar,is_new_bar,data

    def _setup_last_aggregated_bar(self):
        last_aggregated_bar = {}
        for symbol in self.data_handler.symbol_list:
            last_aggregated_bar[symbol] = None

        return last_aggregated_bar
    
    def _setup_initial_exit_levels(self):
        exit_levels = {}
        for symbol  in self.data_handler.symbol_list:
            exit_levels[symbol] = {
                'stop_loss': np.nan,
                'take_profit': np.nan
            }
        return exit_levels
    
    def _initialize_active_trades(self):
        active_trades = {}
        for symbol in self.data_handler.symbol_list:
            active_trades[symbol] = None

        return active_trades

    def _setup_initial_bought(self):
        bought = {}
        for symbol in self.data_handler.symbol_list:
            bought[symbol] = False

        return bought
    
    def sell(self, symbol, date, sell_price, quantity, reason):
        """
        Executes a sell operation for a given stock symbol.
        
        Args:
            symbol (str): The stock symbol for which the sell operation is to be executed.
            date: The date/time when the sell operation is executed.
            sell_price (float or None): The price at which to sell. None for market price.
            quantity (int): The quantity of stocks to be sold.
            reason (str): The reason for executing the sell operation, used for logging purposes.
        
        Returns:
            SignalEvent: An event object representing the sell signal, including the stock symbol, date, 
                         signal type ('SHORT'), quantity, and sell price.
        
        Notes:
            - The function logs the sell signal along with the provided reason.
            - It resets the exit levels for the stock after the sell operation.
            - Updates the bought status to False for the symbol.
            - The function also updates properties for plotting purposes.
        """
        
        logging.info(f"Raising Signal [{date}]: SELL ({reason}) for {symbol}, Qty: {quantity}\n")

        self.add_property_for_plotting(symbol, date, "signal", "SELL")
        self.bought[symbol] = False
        signal = SignalEvent(symbol, date, 'SHORT', quantity, sell_price)

        self.close_trade(symbol, date, sell_price)

        return signal
    
    def buy(self, symbol, date, buy_price, quantity, reason):
        """
        Executes a buy operation for a given stock symbol.
        
        Args:
            symbol (str): The stock symbol for which the buy operation is to be executed.
            date: The date/time when the buy operation is executed.
            buy_price (float): The price at which the stock is being bought.
            quantity (int): The quantity of stocks to be bought.
            reason (str): The reason for executing the buy operation, used for logging purposes.
        
        Returns:
            SignalEvent: An event object representing the buy signal, including the stock symbol, date, 
                         signal type ('LONG'), and quantity.
        
        Notes:
            - The function logs the buy signal along with the provided reason.
            - If the system is in backtest mode, the exit levels for the stock are updated based on the buy price.
            - Updates the bought status to True for the symbol.
            - The function also updates properties for plotting purposes.
        """

        logging.info(f"Raising Signal [{date}]: BUY ({reason}) for {symbol}, Qty: {quantity}\n")

        self.add_property_for_plotting(symbol, date, "signal", "BUY")
        self.bought[symbol] = True
        #todo (maybe): send buy price to signal only in backtest mode
        signal = SignalEvent(symbol, date, 'LONG', quantity, buy_price)

        self.enter_trade(symbol, date, quantity, buy_price)
    
        return signal
    
    def enter_trade(self, symbol, start_time, quantity, buy_price):
        trade = Trade(symbol, quantity, start_time=start_time, buy_price=buy_price)
        self.active_trades[symbol] = trade

    def close_trade(self, symbol, end_date, sell_price):
        trade: Trade = self.active_trades.get(symbol)
        if trade is not None:
            trade.close_trade(end_date, sell_price)
            self.trades.append(trade)
            self.active_trades[symbol] = None

    def add_property_for_plotting(self, symbol, date, property_name, property_value):
        dt = pd.Timestamp(date)
        all_data_df = self.data_handler.all_data[symbol]
        all_data_df.loc[all_data_df['date'] == dt, property_name] = property_value

    def strategy_performance(self):
        self.save_trades_to_csv()
        self.save_trades_results()

    def save_trades_to_csv(self):
        # Directory path
        dir = "performance"
        # Remove directory if it exists
        if not os.path.exists(dir):
            os.makedirs(dir)

        trades_data = []
        for trade in self.trades:
            trades_data.append({
                'Symbol': trade.symbol,
                'Start Time': trade.start_time,
                'End Time': trade.end_time,
                'Duration': (trade.end_time - trade.start_time).total_seconds() / 60 if trade.end_time and trade.start_time else None,
                'Quantity': trade.quantity,
                'Buy Price': round(trade.buy_price, 2),
                'Sell Price': round(trade.sell_price, 2),
                'PnL': round(((trade.sell_price - trade.buy_price) * trade.quantity if trade.sell_price and trade.buy_price else None), 2),
                'Percentage Return': round((((trade.sell_price - trade.buy_price) / trade.buy_price) * 100 if trade.sell_price and trade.buy_price else None), 2)
            })
        trades_df = pd.DataFrame(trades_data)
        trades_df.to_csv(f"{dir}/trades_summary.csv", index=False)

    def save_trades_results(self):
        dir = "performance"
        if not os.path.exists(dir):
            os.makedirs(dir)

        metrics = self.compute_trade_metrics()
        self.write_trade_metrics_to_csv(metrics, filename=f"{dir}/trade_metrics.csv")

    def write_trade_metrics_to_csv(self, metrics: dict, filename):
        df = pd.DataFrame([metrics])
        df.to_csv(filename, index=False)


    def compute_trade_metrics(self) -> dict:
        if not self.trades:
            return {
                "total_trades": 0,
                "num_winning_trades": 0,
                "num_losing_trades": 0,
                "win_loss_ratio": 0,
                "avg_percent_gain_winners": 0,
                "avg_percent_loss_losers": 0,
                "avg_absolute_gain_winners": 0,
                "avg_absolute_loss_losers": 0,
                "avg_return_percent_all_trades": 0,
                "avg_return_absolute_all_trades": 0,
            }

        # Separate winners and losers
        winners = [t for t in self.trades if t.sell_price > t.buy_price]
        losers  = [t for t in self.trades if t.sell_price < t.buy_price]

        # Percentage returns
        percent_returns = [
            (t.sell_price - t.buy_price) / t.buy_price * 100 for t in self.trades
        ]
        percent_winners = [
            (t.sell_price - t.buy_price) / t.buy_price * 100 for t in winners
        ]
        percent_losers = [
            (t.sell_price - t.buy_price) / t.buy_price * 100 for t in losers
        ]

        # Absolute returns (P&L)
        absolute_returns = [
            (t.sell_price - t.buy_price) * t.quantity for t in self.trades
        ]
        absolute_winners = [
            (t.sell_price - t.buy_price) * t.quantity for t in winners
        ]
        absolute_losers = [
            (t.sell_price - t.buy_price) * t.quantity for t in losers
        ]

        num_winners = len(winners)
        num_losers  = len(losers)

        # Metrics
        win_loss_ratio = num_winners / num_losers if num_losers > 0 else float('inf')

        avg_percent_gain_winners = sum(percent_winners) / num_winners if num_winners > 0 else 0
        avg_percent_loss_losers = sum(percent_losers) / num_losers if num_losers > 0 else 0

        avg_absolute_gain_winners = sum(absolute_winners) / num_winners if num_winners > 0 else 0
        avg_absolute_loss_losers = sum(absolute_losers) / num_losers if num_losers > 0 else 0

        # New metrics: average return per trade
        avg_return_percent_all = sum(percent_returns) / len(percent_returns)
        avg_return_absolute_all = sum(absolute_returns) / len(absolute_returns)

        return {
            "total_trades": len(self.trades),
            "num_winning_trades": num_winners,
            "num_losing_trades": num_losers,
            "win_loss_ratio": round(win_loss_ratio, 2),
            "avg_percent_gain_winners": round(avg_percent_gain_winners, 2),
            "avg_percent_loss_losers": round(avg_percent_loss_losers, 2),
            "avg_absolute_gain_winners": round(avg_absolute_gain_winners, 2),
            "avg_absolute_loss_losers": round(avg_absolute_loss_losers, 2),
            "avg_return_percent_all_trades": round(avg_return_percent_all, 2),
            "avg_return_absolute_all_trades": round(avg_return_absolute_all, 2),
        }
    
    def plot_candlestick(self):
        output_dir_all_data = "all_data"
        if os.path.exists(output_dir_all_data):
            shutil.rmtree(output_dir_all_data)
        os.makedirs(output_dir_all_data)

        output_dir_charts = "charts"
        if os.path.exists(output_dir_charts):
            shutil.rmtree(output_dir_charts)
        os.makedirs(output_dir_charts)

        for key, df in self.data_handler.all_data.items():
            file_path_all_data = os.path.join(output_dir_all_data, key + ".xlsx")
            df.to_excel(file_path_all_data, index=False)
            self.plot_daily_candlestick(df, key, output_dir_charts)