import backtrader
class Strategy(backtrader.Strategy):
    def __init__(self):
        self.mom = backtrader.indicators.Momentum(period=12)
    def next(self):
        if not self.position\
        and self.mom[0]>3:
            self.buy()
        elif self.mom[0]<-3:
            self.close()
if __name__ == "__main__":
    import pathlib,sys;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
    import database,datetime
    cerebro = backtrader.Cerebro(stdstats=False)
    cerebro.addstrategy(Strategy)
    cerebro.broker.setcash(1000)
    cerebro.addsizer(backtrader.sizers.PercentSizer, percents=100)
    cerebro.addobserver(backtrader.observers.BuySell,barplot=True,bardist=0.001)  # buy / sell arrows
    cerebro.addobserver(backtrader.observers.DrawDown)
    cerebro.addobserver(backtrader.observers.Broker)
    cerebro.addobserver(backtrader.observers.Trades)
    sym = database.session.query(database.Symbol).filter_by(ticker='ASML.AS').first()
    data = sym.GetData(datetime.datetime.utcnow()-datetime.timedelta(days=30*5))
    cerebro.adddata(backtrader.feeds.PandasData(dataname=data))
    cerebro.run()
    cerebro.plot()
