from events.fill_event import FillEvent
from execution_handler.execution_handler import ExecutionHandler
from datetime import datetime

class SimulateExecutionHandler(ExecutionHandler):
    def __init__(self, events, verbose=False):
        self.events = events
        self.verbose = verbose
        self.fill_listeners = []

    def add_fill_listener(self, listener):
        self.fill_listeners.append(listener)

    def notify_fill_listeners(self, symbol, direction, fill_price):
        for callback in self.fill_listeners:
            callback(symbol, direction, fill_price)

    def execute_order(self, event):
        if event.type == 'ORDER':
            if self.verbose: print("Order Executed:", "Symbol:", event.symbol, "Qty:", event.quantity, event.direction)
            fill_event = FillEvent(datetime.utcnow(), event.symbol, 'ARCA', event.quantity, event.direction, event.price)
            #self.notify_fill_listeners(event.symbol, event.direction, event.price)
            self.events.put(fill_event)