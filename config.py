import dotenv
import os


# Load environment variables from a .env file
dotenv.load_dotenv()

engine_name = os.getenv('SQLITE_ENGINE_NAME')
db_management_engine_name = os.getenv('DB_MANAGEMENT_ENGINE_NAME')
bar_granularity = os.getenv('BAR_GRANULARITY')  # '5 M' , '15 M', '30 M', '1 H'
#bar_granularity = helper.convert_bar_granularity_to_seconds(bar_granularity)
daily_cutoff_time_str = os.getenv('DAILY_TRADING_END_TIME')  ## should be included in the time steps of the chosen bar granularity
backtest_end_date_str = os.getenv('BACKTEST_END_DATE')  # excluding this day
backtest_time_period = int(os.getenv('BACKTEST_TIME_PERIOD'))  # days before range end
is_filter_enabled = os.getenv('IS_FILTER_ENABLED') == '1'
filter_long_sma = int(os.getenv('FILTER_LONG_SMA'))  # days before range start
filter_short_sma = int(os.getenv('FILTER_SHORT_SMA'))
num_of_stocks = int(os.getenv('NUM_OF_STOCKS', '-1'))  # -1 means all available stocks in db


mkt_open_time = os.getenv('MKT_OPEN_TIME', '15:30:00')
mkt_close_time = os.getenv('MKT_CLOSE_TIME', '22:00:00')
daily_trading_end_time = os.getenv('DAILY_TRADING_END_TIME', '21:00')  ## should be included in the time steps of the chosen bar granularity
is_backtest = os.getenv('IS_BACKTEST', '1') == '1' # '1' for True, '0' for False


filter_float_limit = int(os.getenv('FILTER_FLOAT_LIMIT', '100000'))
filter_volume_days = int(os.getenv('FILTER_VOLUME_DAYS', '3'))
filter_volume_multiplier = float(os.getenv('FILTER_VOLUME_MULTIPLIER', '1'))
filter_sma_close_multiplier = float(os.getenv('FILTER_SMA_CLOSE_MULTIPLIER', '1.0'))
filter_daily_performance_criteria = os.getenv('FILTER_DAILY_PERFORMANCE_CRITERIA', 'Strong')
enable_gap_up_filter = os.getenv('ENABLE_FILTER_GAP_UP', '0') == '1'
filter_gap_up_percentage = float(os.getenv('FILTER_GAP_UP_PERCENTAGE', '10'))


initial_capital = float(os.getenv('INITIAL_CAPITAL', '100000'))

ema_short_period = int(os.getenv('EMA_SHORT_PERIOD'))
ema_long_period = int(os.getenv('EMA_LONG_PERIOD'))
take_profit_percentage = float(os.getenv('TAKE_PROFIT_PERCENTAGE'))
enable_rsi_indicator = os.getenv('ENABLE_RSI_INDICATOR', '0') == '1'
rsi_period = int(os.getenv('RSI_PERIOD'))
rsi_overbought = int(os.getenv('RSI_OVERBOUGHT'))
rsi_oversold = int(os.getenv('RSI_OVERSOLD'))



dst_date_change_start = os.getenv('DST_DATE_CHANGE_START')  # '2023-03-12'
dst_date_change_end = os.getenv('DST_DATE_CHANGE_END')      # '2023-11-05'


stop_loss_percentage = float(os.getenv('STOP_LOSS_PERCENTAGE', '3.0'))
reward_risk_ratio = float(os.getenv('REWARD_RISK_RATIO', '3.0'))
opening_range_window_bars = int(os.getenv('OPENING_RANGE_WINDOW_BARS', '3'))
enable_vwap_entry_condition = os.getenv('ENABLE_VWAP_ENTRY_CONDITION', '0') == '1'

plot_performance_graph = os.getenv('PLOT_PERFORMANCE_GRAPH', '1') == '1'