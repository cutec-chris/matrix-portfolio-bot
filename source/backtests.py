import backtrader,asyncio,concurrent.futures,database,datetime,pandas,os,logging,sqlalchemy
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
class BotCerebro(backtrader.Cerebro):
    def __init__(self):
        super().__init__()
        self.internalstart = None
    def plot(self, plotter=None, numfigs=1, iplot=True, start=None, end=None, width=16, height=9, dpi=300, tight=True, use=None, **kwargs):
        if start == None and self.internalstart:
            start = self.internalstart
        return super().plot(plotter, numfigs, iplot, start, end, width, height, dpi, tight, use, **kwargs)
    def saveplots(cerebro, numfigs=1, iplot=True, start=None, end=None,
                width=160*4, height=90*4, dpi=100, tight=True, use=None, file_path = '', **kwargs):
        try:
            from backtrader import plot
            if cerebro.p.oldsync:
                plotter = plot.Plot_OldSync(**kwargs)
            else:
                plotter = plot.Plot(**kwargs)
            import matplotlib,matplotlib.pyplot
            matplotlib.use('AGG')
            matplotlib.pyplot.close('all')
            figs = []
            for stratlist in cerebro.runstrats:
                for si, strat in enumerate(stratlist):
                    rfig = plotter.plot(strat, figid=si * 100,
                                        numfigs=numfigs, iplot=iplot,
                                        start=start, end=end, use=use)
                    figs.append(rfig)
            for fig in figs:
                for f in fig:
                    f.set_size_inches(width / dpi, height / dpi)
                    f.savefig(file_path, dpi=dpi, bbox_inches='tight')
            return figs
        except BaseException as e:
            logger.warning(str(e))
async def run_backtest(cerebro):
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        try:
            return await loop.run_in_executor(executor, cerebro.run)
        except BaseException as e:
            logger.error('failed to execute Strategy: '+str(e))
            return False
async def default_backtest(Strategy=None,ticker=None,isin=None,start=datetime.datetime.utcnow()-datetime.timedelta(days=90),end=None,timeframe='15m',data=None,initial_capital=1000,market=None,depot=None):
    await database.Init(asyncio.get_running_loop())
    data_d = None
    if not isinstance(data, pandas.DataFrame):
        async with database.new_session() as session:
            sym = await database.FindSymbol(session,{'ticker': ticker,'isin': isin},market)
            if sym:
                data = await sym.GetData(session,start,end,timeframe)
                if data.empty:
                    return None,None
            else: return None,None
    if hasattr(Strategy, 'predaysdata'):
        async with database.new_session() as session:
            sym = await database.FindSymbol(session,{'ticker': ticker,'isin': isin},market)
            if sym:
                data_d = await sym.GetData(session,start-datetime.timedelta(days=Strategy.predaysdata),start,timeframe='1d')
    cerebro = BotCerebro(stdstats=False,cheat_on_open=True)
    if hasattr(Strategy, 'predaysdata') and isinstance(data_d, pandas.DataFrame) and not data_d.empty:
        business_days = pandas.date_range(start=data_d.index.min(), end=data_d.index.max(), freq='B')
        data_d = data_d[data_d.index.isin(business_days)]
        data_d = data_d.resample('12H').interpolate(method='linear')
        cerebro.internalstart = data.index.min()
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
    loge = '%s:roi %.2f s-roi:%.2f a-ret: %.2f' % (str(isin)+' ('+str(ticker)+')',roi,sroi,ares)
    if depot:
        loge = '%s:%s' % (depot,loge)
    logger.info(loge)
    return res,cerebro
async def backtest_all(Strategy=None,start=datetime.datetime.utcnow()-datetime.timedelta(days=90),end=None,timeframe='15m',data=None,initial_capital=1000,market=None):
    async with database.new_session() as session:
        syms = (await session.execute(sqlalchemy.select(database.Symbol).filter_by(marketplace=market))).scalars().all()
        for sym in syms:
            data = await sym.GetData(session,start,end,timeframe)
            if not data.empty:
                cerebro = BotCerebro(stdstats=False,cheat_on_open=True)
                if hasattr(Strategy, 'predaysdata'):
                    data_d = await sym.GetData(session,start-datetime.timedelta(days=Strategy.predaysdata),start,timeframe='1d')
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
                if res:
                    annual_returns = res[0].analyzers.annual_return.get_analysis()
                    for year, return_value in annual_returns.items():
                        ares = return_value*100
                else: ares = 0
                logger.info('%s:roi %.2f s-roi:%.2f a-ret: %.2f' % (sym.isin,roi,sroi,ares))
                pass
