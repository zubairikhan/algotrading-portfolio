from datetime import datetime, timedelta

import pandas as pd

import config
from database_repository import DatabaseRepository
import helper


def adjust_for_dst(date, dst_date_change_start, dst_date_change_end):
    if date.date() <= dst_date_change_end.date() and date.date() >= dst_date_change_start.date():
        date = date + timedelta(hours=1)
    return date

def check_for_missing_bars(db_repository, symbol_list, start_time, end_time, dst_date_change_start, dst_date_change_end):
    symbol_records = {}
    rows = db_repository.get_stock_data(start_time, end_time, symbol_list)
    for row in rows:
        bar = list(row)
        date = datetime.strptime(bar[1], "%Y-%m-%d %H:%M:%S")
        date = adjust_for_dst(date, dst_date_change_start, dst_date_change_end)
        bar[1] = date
        stock_symbol = bar[0]
        if stock_symbol not in symbol_records:
            symbol_records[stock_symbol] = []
        symbol_records[stock_symbol].append(bar[1:])

    missing_bars_data = filter_out_stocks_with_missing_records(symbol_records, start_time, end_time)
    return missing_bars_data

@staticmethod
def filter_out_stocks_with_missing_records(symbol_records, start_time, end_time):

    start_date = start_time.strftime("%Y-%m-%d")
    end_date = end_time.strftime("%Y-%m-%d")

    trading_days = helper.get_trading_days(start_date, end_date)
    trading_days = trading_days[:-1]
    expected_bars_per_day = len(helper.expected_times_for_day(trading_days[0])) - 1

    symbols_to_delete = []

    market_open_time = datetime.strptime(helper.MKT_OPEN_TIME, '%H:%M:%S').time()
    market_close_time = datetime.strptime(helper.MKT_CLOSE_TIME, '%H:%M:%S').time()
    missing_bars_data = {}

    for symbol, records in symbol_records.items():
        if not records:
            symbols_to_delete.append(symbol)
            continue

        missing_bars_data[symbol] = {}

        df = pd.DataFrame(records, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['timestamp'].dt.date
        df['time'] = df['timestamp'].dt.time

        # Filter to market hours only
        df = df[
            df['date'].isin(trading_days) &
            (df['time'] >= market_open_time) &
            (df['time'] <= market_close_time)
        ]

        # Count bars per trading day
        daily_counts = df.groupby('date')['timestamp'].nunique()

        for date in trading_days:
            num_missing_bars = expected_bars_per_day - daily_counts.get(date, 0)
            if num_missing_bars > 0:
                missing_bars_data[symbol][date] = num_missing_bars

    return missing_bars_data




    
engine_name = config.db_management_engine_name
database_repository = DatabaseRepository(engine_name)
dst_date_change_start = datetime.strptime(config.dst_date_change_start, "%Y-%m-%d")
dst_date_change_end = datetime.strptime(config.dst_date_change_end, "%Y-%m-%d")
start_time = datetime.strptime("2025-09-11 00:00:00", "%Y-%m-%d %H:%M:%S")
end_time = datetime.strptime("2025-12-02 23:00:00", "%Y-%m-%d %H:%M:%S")



# Example symbol list
symbol_list = database_repository.get_stocks(-1)


# Fetch historical OHLCV data
missing_bars = check_for_missing_bars(database_repository, symbol_list, start_time, end_time, dst_date_change_start, dst_date_change_end)


records = []

for symbol, inner in missing_bars.items():
    for date, missing in inner.items():
        records.append({
            "symbol": symbol,
            "date": date,
            "missing_bars": missing
        })

df = pd.DataFrame(records)
df.to_csv("missing_bars_report.csv", index=False)
