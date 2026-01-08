from datetime import timedelta, datetime
from ibapi.contract import Contract
from ta import trend
import pandas_market_calendars as mcal
import pandas as pd
import config

MKT_OPEN_TIME = config.mkt_open_time
MKT_CLOSE_TIME = config.mkt_close_time
DAILY_TRADING_END_TIME = config.daily_trading_end_time  ## should be included in the time steps of the chosen bar granularity
IS_BACKTEST = config.is_backtest # '1' for True, '0' for False

# def is_backtest():
#     return IS_BACKTEST == '1'

def is_market_closing(curr_time, cutoff_time):
    return curr_time.time() >= (datetime.strptime(MKT_CLOSE_TIME, '%H:%M:%S') - timedelta(minutes=cutoff_time)).time()

def is_trading_cutoff_time(curr_time):
    return curr_time.time() >= datetime.strptime(DAILY_TRADING_END_TIME, '%H:%M').time()


def string_to_datetime(datetime_string):
    date_time = datetime_string.rsplit(' ', 1)[0]
    return datetime.strptime(date_time, '%Y%m%d %H:%M:%S')

def print_all_current_positions(portfolio):
    portfolio.current_positions

def usTechStk(symbol,sec_type="STK",currency="USD",exchange="ISLAND"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    return contract

def get_weekday_before(end_date, days):
    count = 0
    current_date = end_date
    while count < days:
        current_date -= timedelta(days=1)
        if current_date.weekday() < 5:
            count += 1
    return current_date

def is_new_day(data_handler):
    #if IS_BACKTEST:
    data = data_handler.get_latest_data(data_handler.symbol_list[0], 2)
    #else:
     #   data = data_handler.get_latest_data_aggregated(data_handler.symbol_list[0], 2)
    if data is None or len(data) < 2:
        return True
    
    latest = data[-1]

    if latest.datetime.date() != data[0].datetime.date():
        return True

    return False

def calculate_sma(df, col, new_col, period):
    df[new_col] = trend.sma_indicator(df[col], window=period)
    return df

def get_trading_days(start_date, end_date):
    # Define calendar and time range
    nyse = mcal.get_calendar('NYSE')  # or use 'XNAS' for NASDAQ
    schedule = nyse.schedule(start_date, end_date)

    # Extract valid trading days
    trading_days = schedule.index.date

    return trading_days

def get_trading_days_with_early_closes(start_date, end_date):
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date, end_date)
    early_closes = nyse.early_closes(schedule)

    early_close_days = early_closes.index.date.tolist()
    return early_close_days

def get_full_trading_days(start_date, end_date):
    all_trading_days = get_trading_days(start_date, end_date)[:-1]  # Exclude the last day since it's not included in our backtesting range
    early_close_days = get_trading_days_with_early_closes(start_date, end_date)
    full_trading_days = [day for day in all_trading_days if day not in early_close_days]
    return full_trading_days

def expected_times_for_day(date):
    res = pd.date_range(start=f"{date} {MKT_OPEN_TIME}", end=f"{date} {MKT_CLOSE_TIME}", freq='5min')
    return res

def get_expected_number_of_bars_per_day(bar_granularity_in_seconds):
    total_seconds = (datetime.strptime(MKT_CLOSE_TIME, '%H:%M:%S') - datetime.strptime(MKT_OPEN_TIME, '%H:%M:%S')).seconds
    return total_seconds // bar_granularity_in_seconds

def convert_bar_granularity_to_seconds(bar_granularity_in_string):
    quantity_unit = bar_granularity_in_string.split(' ')
    quantity_unit[0] = int(quantity_unit[0])

    match quantity_unit[1]:
        case "M" | "m":
            return int(quantity_unit[0]) * 60
        case "H" | "h":
            return int(quantity_unit[0]) * 60 * 60
        case "S" | "s":
            return int(quantity_unit[0])