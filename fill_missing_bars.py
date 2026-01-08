import sqlite3
import pandas as pd
from datetime import datetime, timedelta


# ----------------------------------------
# CONFIGURATION
# ----------------------------------------
DB_PATH = "your_database.sqlite"
BAR_INTERVAL_MIN = 5  # 5-minute bars
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"   # recommended format


def fetch_stock_id(conn, symbol):
    query = "SELECT id FROM stocks WHERE symbol = ?"
    cur = conn.execute(query, (symbol,))
    row = cur.fetchone()
    return row[0] if row else None


def generate_expected_timestamps(start_ts, end_ts, interval_min=5):
    """Generate list of expected timestamps between start and end range."""
    ts = start_ts
    expected = []
    while ts <= end_ts:
        expected.append(ts)
        ts += timedelta(minutes=interval_min)
    return expected


def fill_missing_bars(df, expected_ts, stock_id):
    """
    For each missing timestamp, create a synthetic flat bar using previous close.
    Returns DataFrame of synthetic rows.
    """

    df = df.sort_values("timestamp")
    df = df.set_index("timestamp")

    synthetic_rows = []

    for ts in expected_ts:
        if ts not in df.index:   # missing bar
            # previous bar must exist, otherwise skip (cannot infer)
            prev_rows = df[df.index < ts]
            if prev_rows.empty:
                # No previous bar for this stock → cannot create synthetic bar
                continue

            prev = prev_rows.iloc[-1]   # last known bar

            synthetic_rows.append({
                "stock_id": stock_id,
                "timestamp": ts.strftime(DATE_FORMAT),
                "open": prev["close"],
                "high": prev["close"],
                "low": prev["close"],
                "close": prev["close"],
                "volume": 0.0,
                "is_synthetic": 1
            })

    return pd.DataFrame(synthetic_rows)


def insert_synthetic_rows(conn, df_synth):
    if df_synth.empty:
        return

    rows = df_synth.to_dict("records")
    query = """
        INSERT INTO stock_prices
        (stock_id, timestamp, open, high, low, close, volume, is_synthetic)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    data = [
        (
            r["stock_id"],
            r["timestamp"],
            r["open"],
            r["high"],
            r["low"],
            r["close"],
            r["volume"],
            r["is_synthetic"]
        )
        for r in rows
    ]

    conn.executemany(query, data)
    conn.commit()


def fill_missing_bars_for_symbol(conn, symbol, start_date, end_date):
    """
    Main function for a single stock symbol.
    - Fetches real bars
    - Computes expected timestamps
    - Generates synthetic bars
    - Inserts them back into DB
    """

    stock_id = fetch_stock_id(conn, symbol)
    if stock_id is None:
        print(f"Symbol {symbol} not found in DB.")
        return

    # Fetch real bars
    query = """
        SELECT stock_id, date, open, high, low, close, volume, is_synthetic
        FROM stock_data_5m
        WHERE stock_id = ? AND date BETWEEN ? AND ?
        ORDER BY date
    """

    df = pd.read_sql_query(query, conn, params=[stock_id, start_date, end_date])

    if df.empty:
        print(f"No data for {symbol} in this date range.")
        return

    # Convert timestamps to datetime
    df["timestamp"] = pd.to_datetime(df["date"])

    start_ts = df["timestamp"].min()
    end_ts = df["timestamp"].max()

    # Generate expected timestamps
    expected_ts = generate_expected_timestamps(start_ts, end_ts, BAR_INTERVAL_MIN)

    # Find missing bars
    df_synth = fill_missing_bars(df, expected_ts, stock_id)

    # Insert synthetic bars
    insert_synthetic_rows(conn, df_synth)

    print(f"{symbol}: Added {len(df_synth)} synthetic bars.")


# ----------------------------------------
# MAIN EXECUTION
# ----------------------------------------

if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)

    # Example usage:
    symbols = ["AAPL", "TSLA", "MSFT"]
    start_date = "2024-02-01 00:00:00"
    end_date = "2024-02-28 23:59:59"

    for sym in symbols:
        fill_missing_bars_for_symbol(conn, sym, start_date, end_date)

    conn.close()
