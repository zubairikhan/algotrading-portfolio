from strategies.strategy import Strategy


class MAStrategy(Strategy):
    def __init__(self, data, events, portfolio, cutoff_time, is_backtest=True):
        super().__init__(data, events, portfolio, cutoff_time, is_backtest)
        self.name = 'MA Strategy'
        self.short_period = 0
        self.long_period = 0
        self.bought = self._setup_initial_bought()
        self.exit_levels = self._setup_initial_exit_levels()
        self.last_aggregated_bar = self._setup_last_aggregated_bar()
        self.take_profit_margin = 0
        self.last_day_bought = []
        