from collections import deque
from datetime import datetime
from ibapi.common import RealTimeBar

class BarAggregator:
    def __init__(self, symbol, on_completed_bar, source_granularity = 5, target_granularity = 10):
        self.aggregation_seconds = target_granularity # bar granularity in seconds
        self.current_aggregated_bar = None
        self.bars_needed = self.aggregation_seconds // source_granularity
        self.bar_buffer = deque(maxlen=self.bars_needed)  # Stores 5-sec bars (60 bars = 5 mins)
        self.last_completed_aggregated_time = None
        self.symbol = symbol
        self.on_completed_bar = on_completed_bar

    def process_bar_for_aggregation(self, bar: RealTimeBar):

        timestamp = bar.time
        open_ = bar.open_
        high = bar.high
        low = bar.low
        close = bar.close
        volume = int(bar.volume)

        """Process incoming finer bar and emit aggregated bars when complete"""
        # Convert timestamp to minutes floor (for alignment)
        bar_time = int(timestamp / self.aggregation_seconds) * self.aggregation_seconds  # Rounds down to nearest interval

        # Initialize new aggregated bar if needed
        if self.current_aggregated_bar is None or bar_time != self.current_aggregated_bar['date']:
            if self.current_aggregated_bar is not None:
                self._finalize_aggregated_bar()

            self.current_aggregated_bar = {
                'date': bar_time,
                'open': open_,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume
            }
        else:
            # Update current aggregated bar with new finer granularity data
            self.current_aggregated_bar['high'] = max(self.current_aggregated_bar['high'], high)
            self.current_aggregated_bar['low'] = min(self.current_aggregated_bar['low'], low)
            self.current_aggregated_bar['close'] = close
            self.current_aggregated_bar['volume'] += volume

        # Store the aggregated bar for reference
        self.bar_buffer.append({
            'date': timestamp,
            'open': open_,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume
        })

    def _finalize_aggregated_bar(self):
        """Called when a complete aggregated bar is ready"""
        if self.current_aggregated_bar is None:
            return

        # Ensure we don't process the same bar twice
        if (self.last_completed_aggregated_time is not None and
                self.current_aggregated_bar['date'] <= self.last_completed_aggregated_time):
            return

        # print(f"\n{self.symbol} - Aggregated Bar Complete: {time.ctime(self.current_aggregated_bar['date'])}")
        # print(f"H: {self.current_aggregated_bar['high']:.2f} "
        #       f"L: {self.current_aggregated_bar['low']:.2f} "
        #       f"O: {self.current_aggregated_bar['open']:.2f} "
        #       f"C: {self.current_aggregated_bar['close']:.2f} "
        #       f"V: {self.current_aggregated_bar['volume']}")

        self.last_completed_aggregated_time = self.current_aggregated_bar['date']
        completed_bar = self.current_aggregated_bar.copy()
        self.current_aggregated_bar = None

        #send completed_bar to callback
        completed_bar['date'] = datetime.fromtimestamp(completed_bar['date'])
        self.on_completed_bar(self.symbol, completed_bar)