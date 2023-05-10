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
                self.sell()
if __name__ == "__main__":
    import pathlib,sys;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
    import database,datetime,backtests,asyncio,logging
    logging.basicConfig(level=logging.DEBUG)
    res,cerebro = asyncio.run(backtests.default_backtest(Strategy,ticker='RWE',market='gettex'))
    cerebro.plot()
    print(res)