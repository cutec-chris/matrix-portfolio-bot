import backtrader
class Strategy(backtrader.Strategy):
    params = (
        ('fast_sma_period', 12*8),
        ('slow_sma_period', 26*8),
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
    import database,datetime,backtests,asyncio,logging
    logging.basicConfig(level=logging.INFO)
    #res,cerebro = asyncio.run(backtests.default_backtest(Strategy,ticker='RWE',market='gettex'))
    #res,cerebro = asyncio.run(backtests.default_backtest(Strategy,isin='FR0000052292',market='gettex'))
    #cerebro.plot()
    asyncio.run(backtests.backtest_all(Strategy,market='gettex'))