from abc import ABCMeta, abstractmethod


class DataHandler(metaclass=ABCMeta):
    """
        DataHandler is an abstract base class providing an interface for
        all subsequent (inherited) data handlers (both live and historic).

        The goal of a (derived) DataHandler object is to output a generated
        set of bars (OLHCVI) for each symbol requested.

        This will replicate how a live strategy would function as current
        market data would be sent "down the pipe". Thus a historic and live
        system will be treated identically by the rest of the backtesting suite.
        """
    __metaclass__ = ABCMeta

    @abstractmethod
    #from documuntation: get_latest_bars
    def get_latest_data(self, symbol, N=1):
        """
               Returns the last N bars from the latest_symbol list,
               or fewer if less bars are available.
               """
        raise NotImplementedError

    @abstractmethod
    #from documuntation: update_bars
    def update_latest_data(self):
        """
               Pushes the latest bar to the latest symbol structure
               for all symbols in the symbol list.
               """
        raise NotImplementedError

    @abstractmethod
    def handle_termination(self):
        """
               Handles any cleanup operations at the termination of a strategy.
               """
        raise NotImplementedError