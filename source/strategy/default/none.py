import backtrader
class Strategy(backtrader.Strategy):
    def next(self):
        if not self.position:
            self.buy(size=self.broker.getcash()/self.data.close[0])