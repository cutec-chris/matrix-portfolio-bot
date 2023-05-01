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
        if not self.position and self.data.close[0] < self.bb.bot[0]:
            self.buy()
        elif self.data.close[0] > self.bb.top[0]:
            self.close()
if __name__ == "__main__":
    import pathlib,sys;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
    import database,datetime,backtests,asyncio,logging
    logging.basicConfig(level=logging.DEBUG)
    res,cerebro = asyncio.run(backtests.default_backtest(Strategy,ticker='RWE'))
    cerebro.plot()
    print(res)