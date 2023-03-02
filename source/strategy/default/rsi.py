import backtrader as bt
class Strategy(bt.Strategy):
    def __init__(self):
        self.rsi = bt.indicators.RSI_SMA(self.data.close, period=21)
    def next(self):
        if not self.position:
            if self.rsi < 30:
                self.buy()
        else:
            if self.rsi > 70:
                self.close()
if __name__ == "__main__":
    import pathlib,sys;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
    import database,datetime
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addstrategy(Strategy)
    cerebro.broker.setcash(1000)
    cerebro.addsizer(bt.sizers.PercentSizer, percents=100)
    cerebro.addobserver(bt.observers.BuySell,barplot=True,bardist=0.001)  # buy / sell arrows
    cerebro.addobserver(bt.observers.Broker)
    cerebro.addobserver(bt.observers.Trades)
    sym = database.session.query(database.Symbol).filter_by(ticker='ASML.AS').first()
    data = sym.GetData(datetime.datetime.utcnow()-datetime.timedelta(days=30*3))
    cerebro.adddata(bt.feeds.PandasData(dataname=data))
    cerebro.run()
    cerebro.plot()