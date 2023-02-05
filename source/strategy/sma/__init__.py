import backtest,pandas_ta
class Strategy(backtest.Strategy):
    async def next(self,df):
        df['SMA_fast'] = pandas_ta.sma(df['Close'],10)
        df['SMA_slow'] = pandas_ta.sma(df['Close'],60)
        if df.iloc[-1]['SMA_slow'] and df.iloc[-1]['SMA_fast']:
            if df.iloc[-1]['SMA_fast'] > df.iloc[-1]['SMA_slow']:
                await self.buy()
            elif df.iloc[-1]['SMA_fast'] < df.iloc[-1]['SMA_slow']:
                await self.sell()