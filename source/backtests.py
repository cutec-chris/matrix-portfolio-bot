import backtrader,asyncio,concurrent.futures,database,datetime
async def run_backtest(cerebro):
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        return await loop.run_in_executor(executor, cerebro.run)
async def default_backtest(Strategy=None,ticker=None,isin=None,start=datetime.datetime.utcnow()-datetime.timedelta(days=30),end=None,timeframe='15m',data=None,initial_captial=1000):
    if data == None:
        async with database.new_session() as session:
            sym = await database.FindSymbol(session,{'ticker': ticker,'isin': isin},None)
            if sym:
                data = await sym.GetData(session,start,end,timeframe)
                if data.empty:
                    return None,None
            else: return None
    cerebro = backtrader.Cerebro(stdstats=False,cheat_on_open=True)
    cerebro.adddata(backtrader.feeds.PandasData(dataname=data))
    cerebro.broker.setcash(initial_captial)
    cerebro.addsizer(backtrader.sizers.PercentSizer, percents=33.3)
    cerebro.addobserver(backtrader.observers.BuySell,barplot=True,bardist=0.001)  # buy / sell arrows
    cerebro.addobserver(backtrader.observers.Broker)
    cerebro.addobserver(backtrader.observers.Trades)
    if Strategy:
        cerebro.addstrategy(Strategy)
    return await run_backtest(cerebro),cerebro