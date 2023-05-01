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
    import database,datetime,backtests,asyncio,logging
    logging.basicConfig(level=logging.DEBUG)
    res,cerebro = asyncio.run(backtests.default_backtest(Strategy,ticker='RWE'))
    cerebro.plot()
    print(res)