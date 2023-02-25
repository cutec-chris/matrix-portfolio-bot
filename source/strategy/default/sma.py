import backtrader
class Strategy(backtrader.Strategy):
    params = (
        ('fast_sma_period', 12*2),
        ('slow_sma_period', 26*2),
    )
    def __init__(self):
        self.fast_sma = backtrader.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.fast_sma_period
        )
        self.slow_sma = backtrader.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.slow_sma_period
        )

    def next(self):
        if not self.position\
        and self.fast_sma[0] > self.slow_sma[0]:
            self.buy(size=self.broker.getcash()/self.data.close[0])
        elif self.fast_sma[0] < self.slow_sma[0]:
            self.close()
if __name__ == "__main__":
    import pathlib,sys;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
    import database,datetime
    cerebro = backtrader.Cerebro(stdstats=False)
    cerebro.addstrategy(Strategy)
    cerebro.broker.setcash(1000)
    cerebro.addobserver(
        backtrader.observers.BuySell,
        barplot=True,
        bardist=0.001)  # buy / sell arrows
    cerebro.addobserver(backtrader.observers.DrawDown)
    #cerebro.addobserver(backtrader.observers.DataTrades)
    cerebro.addobserver(backtrader.observers.Broker)
    cerebro.addobserver(backtrader.observers.Trades)
    sym = database.session.query(database.Symbol).filter_by(ticker='ASML.AS').first()
    data = sym.GetData(datetime.datetime.utcnow()-datetime.timedelta(days=30*5))
    cerebro.adddata(backtrader.feeds.PandasData(dataname=data))
    cerebro.run()
    cerebro.plot()
