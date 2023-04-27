import pathlib,sys;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
import sys,pathlib;sys.path.append(str(pathlib.Path(__file__).parent / 'pyonvista' / 'src'))
import pyonvista,asyncio,aiohttp,datetime,pytz,time,logging,database,pandas,json,aiofiles,datetime,sqlalchemy,threading
async def UpdateTicker(paper,market=None):
    started = time.time()
    updatetime = 0.5
    res = False
    olddate = None
    if market != 'gettex':#avoid updating symbols with other markets than supported
        return res,olddate
    async with database.new_session() as session:
        try:
            sym = await database.FindSymbol(session,paper,market)
            if sym == None or (not 'name' in paper) or paper['name'] == None or paper['name'] == paper['ticker']:
                resp = await SearchPaper(paper['isin'])
                if resp:
                    paper['ticker'] = resp['symbol']
                    if 'longname' in resp:
                        paper['name'] = resp['longname']
                    elif 'shortname' in resp:
                        paper['name'] = resp['shortname']
                else:
                    logging.warning('paper '+paper['isin']+' not found !')
                    return False,None
            if 'ticker' in paper and paper['ticker']:
                startdate = datetime.datetime.utcnow()-datetime.timedelta(days=30)
                if sym == None and resp:
                    #initial download
                    sym = database.Symbol(isin=paper['isin'],ticker=paper['ticker'],name=paper['name'],market=database.Market[resp['type'].lower()],marketplace=market,active=True)
                    sym.currency = 'EUR'
                    if market == 'gettex':
                        sym.tradingstart = datetime.datetime.now().replace(hour=7,minute=0)
                        sym.tradingend = datetime.datetime.now().replace(hour=21,minute=0)
                    else:
                        sym.tradingstart = datetime.datetime.now().replace(hour=7,minute=0)
                        sym.tradingend = datetime.datetime.now().replace(hour=21,minute=0)
                        session.add(sym)
                if sym:
                    result = await session.execute(sqlalchemy.select(database.MinuteBar, sqlalchemy.func.max(database.MinuteBar.date)).where(database.MinuteBar.symbol == sym))
                    date_entry, latest_date = result.fetchone()
                    startdate = latest_date
                    if not latest_date:
                        startdate = datetime.datetime.utcnow()-datetime.timedelta(days=30)
                    if (not (sym.tradingstart and sym.tradingend))\
                    or (datetime.datetime.utcnow()-startdate>datetime.timedelta(days=0.8))\
                    or sym.tradingstart.time() <= datetime.datetime.utcnow().time() <= sym.tradingend.time():
                        client = aiohttp.ClientSession()
                        api = pyonvista.PyOnVista()
                        await api.install_client(client)
                        async with client:
                            i = pyonvista.api.Instrument()
                            i.type=str(sym.market).upper()[7:]
                            i.isin = paper['isin']
                            i.notations = []
                            try:
                                instrument = await api.request_instrument(isin=paper['isin'],instrument=i)
                            except:
                                try:
                                    res = await SearchPaper(paper['isin'])
                                    sym.market=database.Market[res['type'].lower()]
                                    i.type=str(sym.market).upper()[7:]
                                    instrument = await api.request_instrument(isin=paper['isin'],instrument=i)
                                except:
                                    return False,None
                            t_market = None
                            if market:
                                for m in instrument.notations:
                                    if m.market.name == market:
                                        t_market = m
                                        break
                            while startdate.date() <= datetime.datetime.utcnow().date():
                                todate = startdate+datetime.timedelta(days=7)
                                if todate>datetime.datetime.now():
                                    todate = None
                                quotes = await api.request_quotes(instrument,notation=t_market,start=startdate,end=todate)
                                if len(quotes)>0:
                                    data = [
                                        {
                                            'Datetime': quote.timestamp,
                                            'Open': quote.open,
                                            'High': quote.high,
                                            'Low': quote.low,
                                            'Close': quote.close,
                                            'Volume': quote.volume,
                                            'Pieces': quote.pieces,
                                        }
                                        for quote in quotes
                                    ]
                                    updatetime = 5
                                    # Erstellen des DataFrames aus der Liste von Dictionaries
                                    df = pandas.DataFrame(data)
                                    pdata = df.dropna()
                                    try:
                                        olddate = await sym.GetActDate(session)
                                        acnt = await sym.AppendData(session,pdata)
                                        res = res or acnt>0
                                        session.add(sym)
                                        if res: 
                                            olddate = pdata['Datetime'].iloc[-1]
                                            logging.info('onvista:'+sym.ticker+' succesful updated '+str(acnt)+' till '+str(pdata['Datetime'].iloc[-1])+' from '+str(olddate))
                                        else:
                                            logging.info('onvista:'+sym.ticker+' no new data')
                                        updatetime = 10
                                    except BaseException as e:
                                        logging.warning('failed writing to db:'+str(e))
                                startdate += datetime.timedelta(days=7)
            if res: await session.commit()
        except BaseException as e:
            logging.error('onvista:'+'failed updating ticker %s: %s' % (str(paper['isin']),str(e)), exc_info=True)
        return res,olddate
def GetUpdateFrequency():
    return 15*60
async def SearchPaper(isin):
    client = aiohttp.ClientSession()
    api = pyonvista.PyOnVista()
    await api.install_client(client)
    async with client:
        instruments = await api.search_instrument(key=isin)
        if len(instruments)>0:
            instrument = await api.request_instrument(instruments[0])
            return {
                'longname': instrument.name,
                'symbol': instrument.symbol,
                'type': instrument.type
            }
    return None
class UpdateTickers:
    def __init__(self, papers, market,name, delay=0, Waittime=60/3) -> None:
        self.papers = papers
        self.market = market
        self.WaitTime = Waittime
        self.Delay = delay
    async def run(self):
        internal_updated = {}
        while True:
            for paper in self.papers:
                started = time.time()
                try:
                    epaper = paper
                    if paper and (not internal_updated.get(epaper['isin']) or internal_updated.get(epaper['isin'])+datetime.timedelta(seconds=self.Delay) < datetime.datetime.now()):
                        res,till = await UpdateTicker(epaper,self.market)
                        if not till: till = datetime.datetime.now()
                        if res: 
                            internal_updated[paper['isin']] = till
                        else:
                            internal_updated[paper['isin']] = datetime.datetime.now()
                        if self.WaitTime-(time.time()-started) > 0:
                            await asyncio.sleep(self.WaitTime-(time.time()-started))
                except BaseException as e:
                    logging.error(str(e))
            await asyncio.sleep(10)
async def StartUpdate(papers,market,name):
    await UpdateTickers(papers,market,name,60*60,60/6).run()
if __name__ == '__main__':
    logging.root.setLevel(logging.DEBUG)
    apaper = {
        "isin": "DE0007037129",
        "count": 0,
        "price": 0,
        "ticker": "RWE",
        "name": "RWE Aktiengesellschaft"
    }
    asyncio.run(StartUpdate([apaper],'gettex',''))