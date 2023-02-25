import datetime
import backtrader

class Strategy(backtrader.Strategy):
    params = (
        ('bb_window', 20),
        ('bb_dev', 2),
    )
    def __init__(self):
        self.bb = backtrader.indicators.BollingerBands(
            self.data.close,
            devfactor=self.params.bb_dev
        )

    def next(self):
        if not self.position and self.data.close[-4] < self.bb.bot[-4] and self.data[-4] < self.data[0]:
            self.buy()
        elif self.data.close[0] > self.bb.top[0]:
            self.close()
if __name__ == "__main__":
    import pathlib,sys;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
    import database
    cerebro = backtrader.Cerebro()
    cerebro.addstrategy(Strategy)
    cerebro.broker.setcash(1000)
    cerebro.addsizer(backtrader.sizers.PercentSizer, percents=100)
    sym = database.session.query(database.Symbol).filter_by(ticker='ASML.AS').first()
    data = sym.GetData(datetime.datetime.utcnow()-datetime.timedelta(days=30*3))
    cerebro.adddata(backtrader.feeds.PandasData(dataname=data))
    cerebro.run()
    cerebro.plot()
