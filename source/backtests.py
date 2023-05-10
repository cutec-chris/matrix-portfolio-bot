import backtrader,asyncio,concurrent.futures,database,datetime,pandas,logging,sqlalchemy
async def run_backtest(cerebro):
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        return await loop.run_in_executor(executor, cerebro.run)
async def default_backtest(Strategy=None,ticker=None,isin=None,start=datetime.datetime.utcnow()-datetime.timedelta(days=90),end=None,timeframe='15m',data=None,initial_capital=1000,market=None):
    data_d = None
    if not isinstance(data, pandas.DataFrame):
        async with database.new_session() as session:
            sym = await database.FindSymbol(session,{'ticker': ticker,'isin': isin},market)
            if sym:
                data = await sym.GetData(session,start,end,timeframe)
                if data.empty:
                    return None,None
            else: return None
    cerebro = database.BotCerebro(stdstats=False,cheat_on_open=True)
    if hasattr(Strategy, 'predaysdata'):
        data_d = await sym.GetData(session,start-datetime.timedelta(days=Strategy.predaysdata),start,timeframe='1d')
        data_d = data_d.resample(timeframe).interpolate(method='linear')
        data = pandas.concat([data_d, data]).sort_index()
    cerebro.adddata(backtrader.feeds.PandasData(dataname=data))
    cerebro.broker.setcash(initial_capital)
    cerebro.addsizer(backtrader.sizers.PercentSizer, percents=33.3)
    cerebro.addobserver(backtrader.observers.BuySell,barplot=True,bardist=0.001)  # buy / sell arrows
    cerebro.addobserver(backtrader.observers.Broker)
    cerebro.addobserver(backtrader.observers.Trades)
    cerebro.addanalyzer(backtrader.analyzers.AnnualReturn, _name='annual_return')
    if Strategy:
        cerebro.addstrategy(Strategy)
    res = await run_backtest(cerebro)
    sroi = (((cerebro.broker.getvalue() - initial_capital) / initial_capital)*100)
    roi = (float(data.iloc[-1]['Close'])-float(data.iloc[0]['Close']))
    annual_returns = res[0].analyzers.annual_return.get_analysis()
    for year, return_value in annual_returns.items():
        ares = return_value*100
    logging.info('%s:roi %.2f s-roi:%.2f a-ret: %.2f' % (sym.isin,roi,sroi,ares))
    return res,cerebro
async def backtest_all(Strategy=None,start=datetime.datetime.utcnow()-datetime.timedelta(days=90),end=None,timeframe='15m',data=None,initial_capital=1000,market=None):
    async with database.new_session() as session:
        syms = (await session.execute(sqlalchemy.select(database.Symbol).filter_by(marketplace=market))).scalars().all()
        for sym in syms:
            data = await sym.GetData(session,start,end,timeframe)
            if not data.empty:
                cerebro = database.BotCerebro(stdstats=False,cheat_on_open=True)
                if hasattr(Strategy, 'predaysdata'):
                    data_d = await sym.GetData(session,start-datetime.timedelta(days=Strategy.predaysdata),start,timeframe='1d')
                    data_d = data_d.resample(timeframe).interpolate(method='linear')
                    data = pandas.concat([data_d, data]).sort_index()
                cerebro.adddata(backtrader.feeds.PandasData(dataname=data))
                cerebro.broker.setcash(initial_capital)
                cerebro.addsizer(backtrader.sizers.PercentSizer, percents=33.3)
                cerebro.addobserver(backtrader.observers.BuySell,barplot=True,bardist=0.001)  # buy / sell arrows
                cerebro.addobserver(backtrader.observers.Broker)
                cerebro.addobserver(backtrader.observers.Trades)
                cerebro.addanalyzer(backtrader.analyzers.AnnualReturn, _name='annual_return')
                if Strategy:
                    cerebro.addstrategy(Strategy)
                res = await run_backtest(cerebro)
                sroi = (((cerebro.broker.getvalue() - initial_capital) / initial_capital)*100)
                roi = (float(data.iloc[-1]['Close'])-float(data.iloc[0]['Close']))
                annual_returns = res[0].analyzers.annual_return.get_analysis()
                for year, return_value in annual_returns.items():
                    ares = return_value*100
                logging.info('%s:roi %.2f s-roi:%.2f a-ret: %.2f' % (sym.isin,roi,sroi,ares))
                pass
