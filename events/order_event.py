from events.event import Event


class OrderEvent(Event):
    """
        Handles the event of sending an Order to an execution system.
        The order contains a symbol (e.g. GOOG), a type (market or limit),
        quantity and a direction.
        """
    def __init__(self, symbol, order_type, quantity, direction, price=0):
        """
                Initialises the order type, setting whether it is
                a Market order ('MKT') or Limit order ('LMT'), has
                a quantity (integral) and its direction ('BUY' or
                'SELL').

                Parameters:
                symbol - The instrument to trade.
                order_type - 'MKT' or 'LMT' for Market or Limit.
                quantity - Non-negative integer for quantity.
                direction - 'BUY' or 'SELL' for long or short.
                """
        self.type = 'ORDER'
        self.symbol = symbol
        self.order_type = order_type
        self.quantity = quantity
        self.direction = direction
        self.price = price

    def print_order(self):
        print("Order: Symbol={0}, Type={1}, Quantity={2}, Direction={3}").format(self.symbol, self.order_type, self.quantity, self.direction)