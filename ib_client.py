import time
from decimal import Decimal

from ibapi.client import EClient
from ibapi.common import BarData, TickerId, RealTimeBar
from ibapi.contract import Contract
from ibapi.execution import Execution
from ibapi.wrapper import EWrapper
from threading import Thread
import regex as re


class IBClient(EClient, EWrapper):

    def __init__(self, host, port, client_id):
        EClient.__init__(self, self)
        self.executionDetails = {}
        self.fundamental_data = {}
        self.connect(host, port, client_id)
        thread = Thread(target=self.run)
        thread.start()
        time.sleep(1)

    # this is called automatically when the client is connected
    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.order_id = orderId

    # use this to fetch subsequent valid IDs
    def nextId(self):
        self.order_id += 1
        return self.order_id

    def set_dependencies(self, data_handler, execution_handler):
        self.data_handler = data_handler
        self.execution_handler = execution_handler

    def error(self, req_id, code, msg, misc=''):
        if code in [2104, 2106, 2158, 2174]:
            print(msg)
        else:
            if code in [162, 200]:
                if self.data_handler is not None:
                    self.data_handler.track_missing_first_bar(req_id)
            print('Error {}: {}'.format(code, msg))

    def historicalData(self, reqId: int, bar: BarData):
        #print(reqId, bar)
        self.data_handler.capture_historical_data(bar, reqId)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print(f"end of data. Start: {start}. End: {end}. ReqID: {reqId}")
        self.data_handler.historical_data_end(reqId)

    def execDetails(self, reqId, contract: Contract, execution: Execution):
        # print("Order Filled:")
        # print(f"Contract: {contract.symbol} {contract.secType} @ {contract.exchange}")
        # print(f"Direction: {execution.side}")
        # print(f"Shares: {execution.shares}")
        # print(f"Executed Price: {execution.price}")
        # print(f"Execution ID: {execution.execId}, OrderID: {execution.orderId}, Time: {execution.time}")
        

        exec_id = execution.execId
        details = {
            "fill_price": execution.price,
            "quantity": execution.shares,
            "order_id": execution.orderId,
            "exchange": execution.exchange,
            "symbol": contract.symbol,
            "time": execution.time,
            "direction": "BUY" if execution.side == "BOT" else "SELL" 
        }
        #executes first
        if exec_id not in self.executionDetails:
            self.executionDetails[exec_id] = details
        #executes 2nd, commision already exists
        else:
            details["commission"] = self.executionDetails[exec_id]["commission"]
            del self.executionDetails[exec_id]
            # Process Fillevent with execution_details + commission
            self.execution_handler.raise_fill_event(details)


    # Commission Report (Commission Details)
    def commissionReport(self, commissionReport):
        exec_id = commissionReport.execId
        commission = commissionReport.commission
        #executes first
        if exec_id not in self.executionDetails:
            self.executionDetails[exec_id] = {
                "commission": commission
            }
        #executes 2nd, price,qty details already exists
        else:
            details = self.executionDetails[exec_id]
            details["commission"] = commission
            del self.executionDetails[exec_id]
            
            self.execution_handler.raise_fill_event(details)

    def fundamentalData(self, reqId:TickerId , data:str):
        #self.fundamental_data[]
        xml_string = data

        # Regex patterns
        ticker_pattern = r"<IssueID Type=\"Ticker\">(.*?)</IssueID>"
        total_float_pattern = r"<SharesOut.*?TotalFloat=\"(.*?)\""

        # Extract values
        ticker = re.search(ticker_pattern, xml_string).group(1)
        total_float = re.search(total_float_pattern, xml_string).group(1)
        print(f"Ticker: {ticker}")
        print(f"Total Float: {total_float}")
        self.fundamental_data[ticker] = {"float": total_float}
        #print(data)

    def realtimeBar(self, reqId: int, time: int, open_: float, high: float, low: float, close: float, volume: Decimal, wap: Decimal, count: int):
        #super().realtimeBar(reqId, time, open_, high, low, close, volume, wap, count)
        #print(reqId, time, open_, high, low, close, volume, wap, count)


        data = RealTimeBar(time=time, open_=open_, high=high, low=low, close=close, volume=volume)

        self.data_handler.capture_live_data(reqId, data)