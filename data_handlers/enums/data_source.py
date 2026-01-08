from enum import Enum

class DataSource(Enum):
    CSV = 1
    IB_HIST = 2
    DB = 3
    IB_LIVE = 4