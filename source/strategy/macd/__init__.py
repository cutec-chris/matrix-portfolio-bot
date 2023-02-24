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
    import database
    cerebro = backtrader.Cerebro()
    cerebro.addstrategy(Strategy)
    cerebro.addsizer(backtrader.sizers.PercentSizer, percents=100)
    cerebro.broker.setcash(1000)
    sym = database.session.query(database.Symbol).filter_by(ticker='TSLA').first()
    data = sym.GetData(datetime.datetime.utcnow()-datetime.timedelta(days=30*3))
    cerebro.adddata(backtrader.feeds.PandasData(dataname=data))
    cerebro.run()
    cerebro.plot()
