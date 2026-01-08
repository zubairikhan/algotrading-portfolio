class Trade():
    def __init__(self, symbol, quantity, start_time=None, buy_price=0.0):
        self.symbol = symbol
        self.quantity = quantity
        self.buy_price = buy_price
        self.sell_price = 0.0
        self.start_time = start_time
        self.end_time = None

    def set_buy_price(self, price):
        self.buy_price = price

    def set_sell_price(self, price):
        self.sell_price = price

    def close_trade(self, end_time, sell_price):
        self.end_time = end_time
        self.sell_price = sell_price

    def __repr__(self):
        return f"Trade(trade_id={self.trade_id}, symbol='{self.symbol}', quantity={self.quantity}, price={self.price}, trade_type='{self.trade_type}')"