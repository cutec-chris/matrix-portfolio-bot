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
if __name__ == "__main__":
    import pathlib,sys;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
    import database,datetime
    cerebro = backtrader.Cerebro()
    cerebro.addstrategy(Strategy)
    cerebro.broker.setcash(1000)
    sym = database.session.query(database.Symbol).filter_by(ticker='TSLA').first()
    data = sym.GetData(datetime.datetime.utcnow()-datetime.timedelta(days=30*3))
    cerebro.adddata(backtrader.feeds.PandasData(dataname=data))
    cerebro.run()
    cerebro.plot()
