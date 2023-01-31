import backtest,pandas_ta
class Strategy(backtest.Strategy):
    def next(self,df):
        df['SMA_fast'] = pandas_ta.sma(df['Close'],50)
        df['SMA_slow'] = pandas_ta.sma(df['Close'],200)
        if df.iloc[-1]['SMA_fast'] > df.iloc[-1]['SMA_slow']:
            self.buy()
        elif df.iloc[-1]['SMA_fast'] < df.iloc[-1]['SMA_slow']:
            self.sell()