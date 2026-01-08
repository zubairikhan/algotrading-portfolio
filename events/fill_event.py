from events.event import Event


class FillEvent(Event):
    """
        Encapsulates the notion of a Filled Order, as returned
        from a brokerage. Stores the quantity of an instrument
        actually filled and at what price. In addition, stores
        the commission of the trade from the brokerage.
        """
    def __init__(self, timeindex, symbol, exchange, quantity, direction, fill_cost, commission=None):
        """
               Initialises the FillEvent object. Sets the symbol, exchange,
               quantity, direction, cost of fill and an optional
               commission.

               If commission is not provided, the Fill object will
               calculate it based on the trade size and Interactive
               Brokers fees.

               Parameters:
               timeindex - The bar-resolution when the order was filled.
               symbol - The instrument which was filled.
               exchange - The exchange where the order was filled.
               quantity - The filled quantity.
               direction - The direction of fill ('BUY' or 'SELL')
               fill_cost - The holdings value in dollars.
               commission - An optional commission sent from IB.
               """
        self.type = 'FILL'
        self.timeindex = timeindex
        self.symbol = symbol
        self.exchange = exchange
        self.quantity = quantity
        self.direction = direction
        self.fill_cost = fill_cost

        #Calculate commission
        if commission is None:
            self.commission = self.calculate_ib_commission()
        else:
            self.commission = commission

    def calculate_ib_commission(self):
        """
                Calculates the fees of trading based on an Interactive
                Brokers fee structure for API, in USD.

                This does not include exchange or ECN fees.

                Based on "US API Directed Orders":
                https://www.interactivebrokers.com/en/index.php?f=commission&p=stocks2
                """
        full_cost = 1.3
        if self.quantity <= 500:
            full_cost = max(1.3, 0.013 * self.quantity)
        else:
            full_cost = max(1.3, 0.008 * self.quantity)
        full_cost = min(full_cost, 0.5 / 100.0 * self.quantity * self.fill_cost)
        return full_cost