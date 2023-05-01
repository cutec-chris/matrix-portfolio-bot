import datetime
import backtrader

class Strategy(backtrader.Strategy):
    params = (
        ('fast_window', 12),
        ('slow_window', 26),
        ('signal_window', 9),
    )
    def __init__(self):
        self.fast_ma = backtrader.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.fast_window)
        self.slow_ma = backtrader.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.slow_window)
        self.macd = backtrader.indicators.MACD(
            self.data.close,
            period_me1=self.params.fast_window,
            period_me2=self.params.slow_window,
            period_signal=self.params.signal_window
        )

    def next(self):
        if not self.position and self.macd.macd[0] > self.macd.signal[0]:
            self.buy()
        elif self.macd.macd[0] < self.macd.signal[0]:
            self.close()
if __name__ == "__main__":
    import pathlib,sys;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
    import database,datetime,backtests,asyncio,logging
    logging.basicConfig(level=logging.DEBUG)
    res,cerebro = asyncio.run(backtests.default_backtest(Strategy,ticker='RWE'))
    cerebro.plot()
    print(res)