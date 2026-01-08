import csv
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import config
from abc import ABCMeta, abstractmethod
from matplotlib import style
from events.order_event import OrderEvent
from performance import calculate_sharpe_ratio, calculate_drawdowns

class Portfolio(metaclass=ABCMeta):
    """
        The Portfolio class handles the positions and market
        value of all instruments at a resolution of a "bar",
        i.e. secondly, minutely, 5-min, 30-min, 60 min or EOD.
        """
    @abstractmethod
    def update_signal(self, event):
        """
               Acts on a SignalEvent to generate new orders
               based on the portfolio logic.
               """
        raise NotImplementedError

    @abstractmethod
    def update_fill(self, event):
        """
                Updates the portfolio current positions and holdings
                from a FillEvent.
                """
        raise NotImplementedError

    @abstractmethod
    def update_timeindex(self, event):
        raise NotImplementedError


class NaivePortfolio(Portfolio):
    """
        The NaivePortfolio object is designed to send orders to
        a brokerage object with a constant quantity size blindly,
        i.e. without any risk management or position sizing. It is
        used to test simpler strategies such as BuyAndHoldStrategy.
        """
    def __init__(self, data, events, strategy_name, filename):
        """
                Initialises the portfolio with bars and an event queue.
                Also includes a starting datetime index and initial capital
                (USD unless otherwise stated).

                Parameters:
                bars - The DataHandler object with current market data.
                events - The Event Queue object.
                start_date - The start date (bar) of the portfolio.
                initial_capital - The starting capital in USD.
                """

        self.data = data
        self.events = events
        #self.symbol_list = self.data.symbol_list
        self.symbol_list_active = []
        self.initial_capital = config.initial_capital
        self.strategy_name = strategy_name
        self.filename = filename


        #all_positions stores a list of all previous positions recorded at the timestamp of a market data event.
        # Position is simply the quantity of the asset
        #online self.all_positions = self.construct_all_positions()
        self.all_positions = []
        #current_positions stores a dictionary containing the current positions for the last market bar update
        self.current_positions = {symbol: 0.0 for symbol in self.data.symbol_list}

       #all_holdings stores the hitorical list of all symbol holdings
        #online self.all_holdings = self.construct_all_holdings()
        self.all_holdings = []
        #current_holdings stores the most up to date dictionary of all symbol holdings values
        self.current_holdings = self.construct_current_holdings()

    def construct_current_holdings(self):
        """
                This constructs the dictionary which will hold the instantaneous
                value of the portfolio across all symbols.
                """
        holdings = {symbol: 0.0 for symbol in self.data.symbol_list}
        holdings['cash'] = self.initial_capital
        holdings['commission'] = 0.0
        holdings['total'] = self.initial_capital
        return holdings

    def update_timeindex(self, event):
        """
                Adds a new record to the positions matrix for the current
                market data bar. This reflects the PREVIOUS bar, i.e. all
                current market data at this stage is known (OLHCVI).

                Makes use of a MarketEvent from the events queue.
                """
        data = {symbol: self.data.get_latest_data(symbol) for symbol in self.data.symbol_list}
        datetime = data[self.data.symbol_list[0]][0].datetime

        # update all positions listing
        positions = self.current_positions.copy()
        positions['datetime'] = datetime
        self.all_positions.append(positions)

        #update all holdings listing
        holdings = self.get_current_holdings(data)
        self.all_holdings.append(holdings)

    def update_positions_from_fill(self, fill):
        """
                Takes a FilltEvent object and updates the position matrix
                to reflect the new position.

                Parameters:
                fill - The FillEvent object to update the positions with.
                """
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        elif fill.direction == 'SELL':
            fill_dir = -1

        self.current_positions[fill.symbol] += fill_dir * fill.quantity

    def update_holdings_from_fill(self, fill):
        """
                Takes a FillEvent object and updates the holdings matrix
                to reflect the holdings value.

                Parameters:
                fill - The FillEvent object to update the holdings with.
                """
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        elif fill.direction == 'SELL':
            fill_dir = -1

        fill_cost =  fill.fill_cost #if fill.fill_cost != 0 else self.data.get_latest_data(fill.symbol)[0].close

        # computing total and symbol holdings here is not actually 'current_holdings' at a given time
        # for that you need to compute them using current_positions and stock value at that given time
        cost = fill_cost * fill_dir * fill.quantity
        self.current_holdings[fill.symbol] = fill_cost * self.current_positions[fill.symbol]
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)

    def update_fill(self, event):
        """
                Updates the portfolio current positions and holdings
                from a FillEvent.
                """
        if event.type == 'FILL':
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)

    def generate_naive_order(self, signal):
        """
                Simply transacts an OrderEvent object as a constant quantity
                sizing of the signal object, without risk management or
                position sizing considerations.

                Parameters:
                signal - The SignalEvent signal information.
                """
        order = None

        symbol = signal.symbol
        direction = signal.signal_type
        quantity = signal.quantity
        price = signal.price

        market_quantity = quantity
        current_quantity = self.current_positions[symbol]
        order_type = 'MKT'

        if direction == 'LONG':
            order = OrderEvent(symbol, order_type, market_quantity, 'BUY', price)
        if direction == 'SHORT':
            order = OrderEvent(symbol, order_type, market_quantity, 'SELL', price)

        # Unused code for EXIT signals
        if direction == 'EXIT' and current_quantity > 0:
            order = OrderEvent(symbol, order_type, market_quantity, 'SELL')
        if direction == 'EXIT' and current_quantity < 0:
            order = OrderEvent(symbol, order_type, market_quantity, 'BUY')

        return order

    def update_signal(self, event):
        """
                Acts on a SignalEvent to generate new orders
                based on the portfolio logic.
                """
        if event.type == 'SIGNAL':
            order_event = self.generate_naive_order(event)
            if order_event is not None:
                self.events.put(order_event)

    def create_equity_curve_dataframe(self):
        """
                Creates a pandas DataFrame from the all_holdings
                list of dictionaries.
                """
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('datetime', inplace=True)
        curve['returns'] = curve['total'].pct_change() #daily return. how much value has changed from prev day
        curve['equity_curve'] = (1.0 + curve['returns']).cumprod() #compounded growth index at that point in time from starting value. shows how an initial investment would grow over time
        self.equity_curve = curve
        self.holdings_curve = curve['total']

    def summary_stats(self, bar_size_in_sec):
        self.create_equity_curve_dataframe()
        total_return = self.equity_curve['equity_curve'][-1] #start_capital * total_return = final total value
        returns = self.equity_curve['returns']
        pnl = self.equity_curve['equity_curve']

        sharpe_ratio = calculate_sharpe_ratio(returns, bar_size_in_sec)
        max_dd, dd_duration = calculate_drawdowns(pnl)

        stats = {
            "Total Return (%)": round((total_return - 1.0) * 100, 2),
            "Sharpe Ratio": round(sharpe_ratio, 2),
            "Max Drawdown (%)": round(max_dd * 100, 2),
            "Drawdown Duration": dd_duration
        }

        # Add current positions (non-zero only)
        for key, value in self.current_positions.items():
            if value != 0:
                stats[f"Position: {key}"] = value

        # Add current holdings (non-zero only)
        for key, value in self.get_current_holdings().items():
            if value != 0:
                stats[f"Holding: {key}"] = value
        
        self.write_summary_stats_to_file(stats)

        
    def write_summary_stats_to_file(self, stats):
        dir = "performance"
        if not os.path.exists(dir):
            os.makedirs(dir)

        csv_path = os.path.join(dir, self.filename + '_performance.csv')
        
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Metric", "Value"])
            for key, value in stats.items():
                writer.writerow([key, value])
        

    def plot_holdings(self):
        #In this plot the total value of the holdings is plotted
        holdings_fig, holdings_ax = plt.subplots()
        self.holdings_curve.plot(ax=holdings_ax)
        holdings_ax.set_title('Holdings')
        holdings_ax.set_xlabel('Time')
        holdings_ax.set_ylabel('Total')
        plt.gcf().set_size_inches(20, 10)
        plt.savefig(self.filename+'_holdings.png')

    def plot_equity(self):
        # #In this plot the total value of the holdings is plotted
        # equity_fig, equity_ax = plt.subplots()
        # self.equity_curve['equity_curve'].plot(ax=equity_ax)
        # equity_ax.set_title('Holdings')
        # equity_ax.set_xlabel('Time')
        # equity_ax.set_ylabel('Total')
        # plt.gcf().set_size_inches(20, 10)
        # plt.savefig(self.filename+'_equity.png')

        # Simplified version

        df = self.equity_curve
        df['equity_curve'].plot(
            title='Equity Curve',
            figsize=(12, 6),
            grid=True,
            color='blue'
        )
        plt.xlabel('Time')
        plt.ylabel('Portfolio Value')
        plt.tight_layout()

        plt.fill_between(
            df.index,
            df['equity_curve'],
            df['equity_curve'].cummax(),
            color='red',
            alpha=0.3,
            label='Drawdowns'
        )

        # plt.scatter(
        #     df.index,
        #     df['equity_curve'],
        #     color='red',
        #     marker='o',
        #     label='Trades'
        # )

        plt.show()

    def plot_equity_v2(self):

        df = self.equity_curve

        print("Date Range:", df.index.min(), "to", df.index.max())

        fig, ax = plt.subplots(figsize=(24, 12))
        df['equity_curve'].plot(ax=ax, title='Equity Curve')

        # Set x-axis limits to your full date range
        ax.set_xlim([df.index.min(), df.index.max()])

        # Format x-axis labels
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))  # Show every day
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

    def plot_performance(self):
        #In this plot the performance of the strategy is compared with buy&hold strategy for each stock
        performance_df = self.data.create_baseline_dataframe()
        performance_df[self.strategy_name] = self.equity_curve['equity_curve'][1:]
        performance_df = (performance_df * 100) - 100
        performance_fig, performance_ax = plt.subplots()
        performance_df.plot(ax=performance_ax)
        performance_ax.set_title('Performance')
        performance_ax.set_xlabel('Time')
        performance_ax.set_ylabel('Return (%)')
        plt.gcf().set_size_inches(20, 10)
        plt.savefig(self.filename+'_performance.png')

    def plot_all(self):
        style.use('ggplot')
        self.create_equity_curve_dataframe()
        self.equity_curve.to_excel(self.filename+'.xlsx')
        self.plot_performance()

        #self.equity_curve.set_index('datetime', inplace=True)

        #self.plot_performance()
        self.plot_equity_v2()
        self.plot_holdings()
       # plt.show()

    def get_current_holdings(self, last_bar=None):
        if last_bar is None:
            last_bar = {symbol: self.data.get_latest_data(symbol) for symbol in self.data.symbol_list}

        holdings = self.current_holdings.copy()
        holdings['datetime'] = last_bar[self.data.symbol_list[0]][0].datetime
        holdings['total'] = self.current_holdings['cash']

        for symbol in self.data.symbol_list:
            market_value = self.current_positions[symbol] * last_bar[symbol][0].close
            holdings[symbol] = market_value
            holdings['total'] += market_value

        return holdings