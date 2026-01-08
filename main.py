import logging
import queue
import signal
import sys
import time
import traceback
from datetime import datetime, timedelta

import config

import pytz

from data_handlers.enums.data_format import DataFormat
from data_handlers.enums.data_source import DataSource
from data_handlers.historic_csv_data_handler import HistoricCSVDataHandler
from data_handlers.historic_db_data_handler import HistoricDBDataHandler
from data_handlers.ib_data_handler import IBDataHandler
from data_handlers.live_data_handler import LiveDataHandler
from database_repository import DatabaseRepository

import helper
from execution_handler.ib_execution_handler import IBExecutionHandler
from execution_handler.simulate_execution_handler import SimulateExecutionHandler

from filters import StockFilter
from loop import backtest, BacktestDependencies
from portfolio import NaivePortfolio
from strategies.orb_strategy import OpeningRangeBreakoutStrategy

logging.basicConfig(
    handlers=[
        logging.FileHandler('logs/app.log', mode='w'),
        logging.StreamHandler()
    ],
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def print_exception(e):
    """Print detailed exception information"""
    print(f"\n=== EXCEPTION DETAILS ===")
    print(f"Type: {type(e).__name__}")
    print(f"Message: {str(e)}")
    print(f"Args: {e.args}")
    print("Traceback (most recent call last):")
    traceback.print_tb(e.__traceback__)


def emergency_stop(is_backtest=None):
    if helper.IS_BACKTEST:
        sys.exit(0)
    else:
        cancel_ib_data_subscription()

def handle_ib_client(data_handler=None):
    if not helper.IS_BACKTEST:
        print("Handling IB CLIENT")
        cancel_ib_data_subscription(data_handler)
        data_handler.ib_client.disconnect()

def cancel_ib_data_subscription(data_handler=None):
    for idx, _ in enumerate(data_handler.symbol_list):
        data_handler.ib_client.cancelRealTimeBars(idx)

def handle_terminate(signum):
    print(f"\nEmergency Stop: Signal {signum} received.")
    emergency_stop()

def initialize_execution_handler(events, is_backtest, strategy, ib_client):
    if is_backtest:
        execution_handler = SimulateExecutionHandler(events, False)
        execution_handler.add_fill_listener(strategy.on_order_filled)
    else:
        execution_handler = IBExecutionHandler(events, ib_client)
        execution_handler.add_fill_listener(strategy.on_order_filled)
    return execution_handler


def initialize_data_handler(data_source, database_repository, events, tickers, bar_granularity=None):

    data = None

    match data_source:
        case DataSource.IB_HIST:
            data = IBDataHandler(events, tickers)
        case DataSource.IB_LIVE:
            data = LiveDataHandler(events, tickers, database_repository, bar_granularity)
        case DataSource.CSV:
            data = HistoricCSVDataHandler(events, 'csv', ['testsymbol4short'], DataFormat.NASDAQ)
        case DataSource.DB:
            data = HistoricDBDataHandler(events, tickers, database_repository, bar_granularity)
    return data


def consume_data_needed_for_filter(data_handler, events, last_timestamp_before_backtest):
    if helper.IS_BACKTEST:# if len(data.symbol_list) > 0:
        while True:
            try:
                data_handler.update_latest_data()
            except Exception as e:
                print(e)
            latest_bar = data_handler.get_latest_data(data_handler.symbol_list[0])
            if latest_bar[0].datetime == last_timestamp_before_backtest:
                break

        while not events.empty():
            events.get()

def wait_until_market_open(bar_granularity, is_filter_enabled):
    """
    Waits until the market opens, with an optional delay to account for the first complete bar of the day.
    This function calculates the time remaining until the market opens based on the configured market open time.
    If the `is_filter_enabled` parameter is set to True, it waits an additional duration equivalent to the 
    bar granularity (in minutes) to ensure the first complete bar of the day is available for analysis.
    Args:
        bar_granularity (int): The granularity of the bar in seconds. Used to calculate the delay after market open.
        is_filter_enabled (bool): A flag indicating whether to enable the delay for the relative volume filter.
    Raises:
        ValueError: If the market open time is not properly configured in `helper.MKT_OPEN_TIME`.
    Side Effects:
        Pauses execution using `time.sleep()` until the calculated wait time has elapsed.
    """
    
    cet = pytz.timezone("CET")
    now = datetime.now(cet)

    hour = int(helper.MKT_OPEN_TIME.split(':')[0])
    minute = int(helper.MKT_OPEN_TIME.split(':')[1])

    market_open = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    wait_until = market_open

    if is_filter_enabled:
        # wait a few minutes after market open to have first complete bar of the day to use for relative volume filter
        wait_until = market_open + timedelta(minutes=bar_granularity/60)
    
    wait_seconds = (wait_until - now).total_seconds()

    if wait_seconds > 0:
        print(f"Waiting {wait_seconds / 60:.2f} minutes...")
        time.sleep(wait_seconds)

def filter_stocks_on_daily_performance_live_trading(tickers, stock_filter: StockFilter):
    filtered_stocks = stock_filter.daily_performance_filter_for_live_trading(tickers)
    filtered_tickers = list(filtered_stocks.keys())
    
    if len(filtered_tickers) == 0:
        raise Exception("No stocks available after strong performing real-time filter. Exiting.")
    return filtered_stocks

def filter_relative_volume_stocks_live_trading(filtered_stocks, stock_filter: StockFilter):
    filtered_stocks = stock_filter.relative_volume_filter_for_live_trading(filtered_stocks)
    filtered_tickers = list(filtered_stocks.keys())
    
    if len(filtered_tickers) == 0:
        raise Exception("No stocks available after relative volume real-time filter. Exiting.")
    return filtered_stocks

def filter_out_stocks_without_day_open_bar(data_handler: LiveDataHandler, filtered_stocks):
    
    missing = data_handler.missing_first_bar
    logging.info(f"Filtering out {missing} stocks without day open bar data. Total stocks before filter: {len(filtered_stocks.keys())}")
    for symbol in missing:
        if symbol in filtered_stocks:
            logging.info(f"Removing {symbol} from trading list as no day open bar found.")
            del filtered_stocks[symbol]


def main():
    
    
    ################ PARAMETERS #######################

    # Both
    engine_name = config.engine_name
    bar_granularity_string = config.bar_granularity  # '5 M' , '15 M', '30 M', '1 H'
    bar_granularity = helper.convert_bar_granularity_to_seconds(bar_granularity_string)
    daily_cutoff_time_str = config.daily_cutoff_time_str or helper.DAILY_TRADING_END_TIME  ## should be included in the time steps of the chosen bar granularity
    backtest_end_date_str = config.backtest_end_date_str  # excluding this day
    backtest_time_period = config.backtest_time_period  # days before range end
    is_filter_enabled = config.is_filter_enabled
    filter_long_sma = config.filter_long_sma  # days before range start
    filter_short_sma = config.filter_short_sma
    num_of_stocks = config.num_of_stocks  # -1 means all available stocks in db



    is_backtest = helper.IS_BACKTEST 

    
    # Both
    logging.info(
        f"""
            Parameters - is_backtest: {is_backtest}, daily_cutoff_time: {daily_cutoff_time_str},
            backtest_end_date: {backtest_end_date_str}, backtest_time_period: {backtest_time_period}, bar_granularity: {bar_granularity}, 
            is_filter_enabled: {is_filter_enabled}, filter_long_sma: {filter_long_sma}, filter_short_sma: {filter_short_sma}
        """
    )


    # BothD
    # Intialize DB Repository and get tickers
    database_repository = DatabaseRepository(engine_name)
    tickers = database_repository.get_stocks(num_of_stocks)



    ##
    # if backtest end date is 2024-12-31 00:00:00 and backtest_time_period is 5
    # then backtest will run for 5 weekdays prior to that, excluding 31st December.
    # and if filter period is 5 days, then we fetch additional 5 days worth of data but we dont run backtest on that - that is only used for filtering conditions

    ############### TIME PROCESSING #########################

    cutoff_time = datetime.strptime(daily_cutoff_time_str, "%H:%M")
    duration_for_hist_data = backtest_time_period + filter_long_sma
    backtest_end_date = datetime.strptime(backtest_end_date_str, "%Y-%m-%d %H:%M:%S")
    backtest_start_date = helper.get_weekday_before(backtest_end_date, backtest_time_period)
    backtest_start_date_str = backtest_start_date.strftime("%Y-%m-%d %H:%M:%S")

    # BacktestD
    #TODO: replace weekday logic with trading day logic using market calendar
    last_timestamp_before_backtest = (helper.get_weekday_before(backtest_start_date, 1)).replace(hour=cutoff_time.hour,
                                                                                                 minute=cutoff_time.minute)  # cutoff time

    if cutoff_time.minute * 60 % bar_granularity != 0:
        raise Exception("Cutoff time minutes must align with bar granularity time steps.")
    

    ############ DATA HANDLER ###################

    data_source = DataSource.DB if is_backtest else DataSource.IB_LIVE

    events = queue.Queue()
    data_handler = initialize_data_handler(data_source, database_repository, events, tickers, bar_granularity)

    try:

        signal.signal(signal.SIGINT, data_handler.handle_termination)

        portfolio = NaivePortfolio(data_handler, events, 'ema', filename="testrun")
        #strategy = EMAStrategy(data_handler, events, portfolio, cutoff_time=60 - cutoff_time.minute)
        strategy = OpeningRangeBreakoutStrategy(data_handler, events, portfolio, cutoff_time=60 - cutoff_time.minute)
        #strategy = EMAAndRSIStrategy(data_handler, events, portfolio, cutoff_time=60 - cutoff_time.minute)
        portfolio.strategy_name = strategy.name
        execution_handler = initialize_execution_handler(events, is_backtest, strategy, data_handler.ib_client if data_source in [DataSource.IB_HIST, DataSource.IB_LIVE] else None)

        data_handler.fetch_float_data()

        stock_filter = None
        if is_filter_enabled:
            stock_filter = StockFilter(
                data_handler, cutoff_time, sma_long_period=filter_long_sma, sma_short_period=filter_short_sma, bar_granularity=bar_granularity
            )
            
            tickers = stock_filter.float_filter()
            data_handler.symbol_list = tickers
            data_handler.symbol_list_active = tickers
            
            if len(tickers) == 0:
                logging.info("No stocks available after float filter. Exiting.")
                sys.exit(0)
            


        ############ FETCH HISTORICAL DATA IF NEEDED ###################

        if type(data_handler) == IBDataHandler:
            data_handler.ib_client.set_dependencies(data_handler, execution_handler)
            data_handler.fetch_historical_data(0, backtest_end_date_str, f'{duration_for_hist_data} D', '5 mins')
            time.sleep(1)
        
        elif type(data_handler) == HistoricDBDataHandler:
            hist_data_end = backtest_end_date
            hist_data_start = helper.get_weekday_before(backtest_end_date, duration_for_hist_data)
            data_handler.fetch_historical_ohlcv_data(hist_data_start, hist_data_end)
            strategy.post_data_fetch_setup()
            time.sleep(1)

        elif type(data_handler) == LiveDataHandler:
            data_handler.ib_client.set_dependencies(data_handler, execution_handler)
            hist_data_end = backtest_end_date
            hist_data_start = helper.get_weekday_before(backtest_end_date, duration_for_hist_data)
            if is_filter_enabled:
                data_handler.fetch_historical_ohlcv_data(hist_data_start, hist_data_end)
            tickers = data_handler.symbol_list
            # data_handler.fetch_live_data(req_id=0)
            # time.sleep(1)

        if len(data_handler.symbol_list) == 0:
            raise Exception("No stocks available after initial data fetch. Exiting.")
        
        consume_data_needed_for_filter(data_handler, events, last_timestamp_before_backtest)


        #filter strong performing stocks for live trading
        if not is_backtest:
            if is_filter_enabled:
                filtered_stocks = filter_stocks_on_daily_performance_live_trading(tickers, stock_filter)
                filtered_tickers = list(filtered_stocks.keys())
                data_handler.symbol_list = filtered_tickers
                data_handler.symbol_list_active = filtered_tickers
            

        
            wait_until_market_open(bar_granularity, is_filter_enabled)

            if is_filter_enabled:
                # fetch first complete bar of the day for all stocks to be able to calculate relative volume for volume filter
                data_handler.fetch_first_bar_of_day(bar_granularity)
                while (data_handler.is_volume_data_complete(len(filtered_tickers)) == False):
                    time.sleep(1)
                filter_out_stocks_without_day_open_bar(data_handler, filtered_stocks)

            if is_filter_enabled:
                filtered_stocks = filter_relative_volume_stocks_live_trading(filtered_stocks, stock_filter)
                filtered_tickers = list(filtered_stocks.keys())
                data_handler.symbol_list = filtered_tickers
                data_handler.symbol_list_active = filtered_tickers


            data_handler.fetch_live_data(req_id=0)
            time.sleep(1)
            #strategy.post_data_fetch_setup()

        

        backtest_dependencies = BacktestDependencies(
            events=events,
            data=data_handler,
            portfolio=portfolio,
            strategy=strategy,
            execution_handler=execution_handler,
            stock_filter=stock_filter,
            tickers=data_handler.symbol_list,
            bar_size_in_sec=bar_granularity,
            is_backtest=is_backtest
        )


        if len(backtest_dependencies.tickers) > 0:
            backtest(backtest_dependencies)
        else:
            logging.info("No stocks available after filtering. Exiting.")

    except Exception as e:
        print_exception(e)
        
    finally:
        data_handler.handle_termination()



if __name__ == "__main__":
    main()
    # try:
    #     main()
    # except Exception as e:
    #     print_exception(e)
    # finally:
    #     handle_ib_client()


