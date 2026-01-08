import numpy as np
import pandas as pd

def calculate_sharpe_ratio(returns, bar_size_in_sec):
    periods = 252 * 6.5 * (3600 / bar_size_in_sec)
    return (np.sqrt(periods) * np.mean(returns)) / np.std(returns)

def calculate_drawdowns(equity_curve):
    hwm = [0]
    eq_index = equity_curve.index
    drawdown = pd.Series(index=eq_index)
    duration = pd.Series(index=eq_index)

    for i in range(1, len(eq_index)):
        current_hwm = max(hwm[i-1], equity_curve[i]) #highest portfolio value observed so far
        hwm.append(current_hwm)
        drawdown[i] = hwm[i] - equity_curve[i]
        duration[i] = 0 if drawdown[i] == 0 else duration[i-1] + 1

    return drawdown.max(), duration.max()
