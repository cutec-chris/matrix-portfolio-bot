import backtrader
class Strategy(backtrader.Strategy):
    params = (
        ('fast_sma_period', 12),
        ('slow_sma_period', 26),
    )

    def __init__(self):
        self.fast_sma = backtrader.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.fast_sma_period
        )
        self.slow_sma = backtrader.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.slow_sma_period
        )

    def next(self):
        if self.data.close[0] > self.fast_sma[0] and self.data.close[-1] <= self.fast_sma[-1]:
            self.buy()
        elif self.data.close[0] < self.slow_sma[0] and self.data.close[-1] >= self.slow_sma[-1]:
            self.sell()