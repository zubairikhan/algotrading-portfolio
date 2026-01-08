from abc import ABCMeta, abstractmethod

class ExecutionHandler(metaclass=ABCMeta):

    @abstractmethod
    def execute_order(self, event):
        raise NotImplementedError