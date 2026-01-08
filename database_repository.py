
import random
import sqlite3


class DatabaseRepository:
    def __init__(self, engine_name):
        self.engine_name = engine_name
        self.stocks_table_name = "stocks"
        self.stock_data_table_name = "stock_data_5m"

    # get all non-blacklisted stocks from our "stocks" table
    # if count is -1, get all stocks, else get random sample of count stocks
    # return list of symbol strings
    def get_stocks(self, count=-1, is_blacklisted=0):
        try:
            symbols = []
            db_conn = sqlite3.connect(self.engine_name)
            db_cursor = db_conn.cursor()
            query = f"select symbol FROM {self.stocks_table_name} where is_blacklisted = {is_blacklisted}"
            db_cursor.execute(query)

            for row in db_cursor.fetchall():
                symbols.append(row[0])

            db_conn.close()

            if count == -1 or count > len(symbols):
                return symbols

            return random.sample(symbols, count)
        
        except Exception as e:
            print("Error fetching data: %s", e)
            return []
    
    # get specific symbols by name
    # symbols_to_get is a list of symbol strings
    # return list of tuples (id, symbol)
    def get_stocks_by_name(self, symbols_to_get):
        try:
            symbols = []
            db_conn = sqlite3.connect(self.engine_name)
            db_cursor = db_conn.cursor()

            query = f"""
            SELECT id, symbol FROM stocks
            where is_blacklisted = 0
            and symbol in ({','.join(['?']*len(symbols_to_get))}) """

            db_cursor.execute(query, symbols_to_get)

            for row in db_cursor.fetchall():
                symbols.append((row[0], row[1]))

            db_conn.close()
            return symbols

        except Exception as e:
            print("Error fetching stocks by name: %s", e)
            return []
    
    
    # get stock data for given symbols between start_time and end_time
    # symbol_list is a list of symbol strings
    # start_time and end_time are strings in format 'YYYY-MM-DD HH:MM:SS'
    # return list of tuples (symbol, date, open, high, low, close, volume)
    # ordered by symbol and date
    def get_stock_data(self, start_time, end_time, symbol_list):
        try:
            db_conn = sqlite3.connect(self.engine_name)
            db_cursor = db_conn.cursor()
            query = f"""
                SELECT 
                s.symbol, 
                sd.date, 
                sd.open, 
                sd.high, 
                sd.low, 
                sd.close, 
                sd.volume
                FROM 
                {self.stock_data_table_name} sd
                INNER JOIN 
                {self.stocks_table_name} s
                ON 
                sd.stock_id = s.id
                WHERE 
                s.symbol IN ({','.join(['?'] * len(symbol_list))})
                AND sd.date BETWEEN '{start_time}' AND '{end_time}'
                ORDER BY 
                s.symbol, 
                sd.date;
                """

            print("Query:" + query)

            db_cursor.execute(query, symbol_list)

            rows = db_cursor.fetchall()
            db_conn.close()
            return rows

        except Exception as e:
            print("Error fetching stock data: %s", e)
            return []
    
    # get stock float for given symbols
    # symbol_list is a list of symbol strings
    # return list of tuples (symbol, stock_float)
    def get_stock_float(self, symbol_list):
        try:
            db_conn = sqlite3.connect(self.engine_name)
            db_cursor = db_conn.cursor()
            query = f"""select symbol, stock_float
            from stocks
            where symbol in ({','.join(['?']*len(symbol_list))});"""

            db_cursor.execute(query, symbol_list)
            rows = db_cursor.fetchall()
            db_conn.close()
            return rows
        except Exception as e:
            print("Error fetching stock float: %s", e)
            return []

    