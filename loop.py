import queue
from dataclasses import dataclass
from queue import Queue
from typing import List
import helper
from data_handlers.data_handler import DataHandler
from execution_handler.execution_handler import ExecutionHandler

from filters import StockFilter
from portfolio import Portfolio
from strategies.strategy import Strategy
import config

@dataclass
class BacktestDependencies:
    events: Queue
    data: DataHandler
    portfolio: Portfolio
    strategy: Strategy
    execution_handler: ExecutionHandler
    stock_filter: StockFilter
    tickers: List[str]
    bar_size_in_sec: int
    is_backtest: bool


def backtest(configuration: BacktestDependencies):
    events = configuration.events
    data = configuration.data
    portfolio = configuration.portfolio
    strategy = configuration.strategy
    broker = configuration.execution_handler
    stock_filter = configuration.stock_filter
    tickers = configuration.tickers
    bar_size_in_sec = configuration.bar_size_in_sec

    while True:
        data.update_latest_data()
        if data.continue_backtest == False:
            break

        if helper.is_new_day(data):
            process_start_of_new_day(data, strategy, stock_filter, tickers)

        while True:
            try:
                event = events.get(block=False)
            except queue.Empty:
                break


            if event is not None:
                if event.type == 'MARKET':
                    strategy.calculate_signals(event)
                    portfolio.update_timeindex(event)
                elif event.type == 'SIGNAL':
                    portfolio.update_signal(event)
                elif event.type == 'ORDER':
                    broker.execute_order(event)
                elif event.type == 'FILL':
                    portfolio.update_fill(event)

    portfolio.summary_stats(bar_size_in_sec)
    strategy.strategy_performance()
    strategy.plot()

    if config.plot_performance_graph:
        portfolio.plot_all()

    strategy.plot_candlestick()


def process_start_of_new_day(data, strategy, stock_filter, tickers):
    if stock_filter is not None and helper.IS_BACKTEST:
        run_daily_stock_filtering_for_backtesting(data, stock_filter, tickers)
    strategy.process_start_of_new_day()


def run_daily_stock_filtering_for_backtesting(data, stock_filter: StockFilter, tickers):
    data.symbol_list_active = stock_filter.filter_stocks_for_backtesting(tickers)
