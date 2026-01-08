from events.fill_event import FillEvent
from ibapi.contract import Contract
from ibapi.order import Order

from execution_handler.execution_handler import ExecutionHandler


class IBExecutionHandler(ExecutionHandler):
    def __init__(self, events, ib_client, verbose=False):
        self.events = events
        self.verbose = verbose
        self.ib_client = ib_client
        self.fill_listeners = []

    def add_fill_listener(self, listener):
        self.fill_listeners.append(listener)

    def notify_fill_listeners(self, symbol, direction, fill_price):
        for callback in self.fill_listeners:
            callback(symbol, direction, fill_price)

    def raise_fill_event(self, exec_details):
        # print("Order Filled:")
        # print(exec_details)
        fill_event = FillEvent(exec_details["time"], exec_details["symbol"], 'SMART', float(exec_details["quantity"]),
                               exec_details["direction"], exec_details["fill_price"], exec_details["commission"])
        self.events.put(fill_event)
        self.notify_fill_listeners(exec_details["symbol"], exec_details["direction"], exec_details["fill_price"])

    def execute_order(self, event):
        contract = Contract()
        contract.symbol = event.symbol
        contract.secType = "STK"
        contract.currency = "USD"
        contract.exchange = "SMART"

        order = Order()
        order.orderType = "MKT"
        order.totalQuantity = event.quantity

        if event.direction == 'BUY':
            order.action = "BUY"

        elif event.direction == 'SELL':
            order.action = "SELL"

        if self.ib_client.order_id:
            # print(f"Execution: Placing {event.direction} order for {contract.symbol}")
            self.ib_client.placeOrder(self.ib_client.nextId(), contract, order)
