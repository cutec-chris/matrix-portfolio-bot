import pathlib,sys;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
import asyncio,aiohttp,csv,datetime,pytz,time,threading,concurrent.futures
import requests,pandas,pathlib,database,sqlalchemy.sql.expression,asyncio,logging,io
UserAgent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36'
async def UpdateTicker(paper,market=None,connection=database.Connection()):
    def extract_trading_times(metadata):
        try:
            timezone = metadata['regular']['timezone']
            if timezone == 'CEST': timezone = 'Europe/Berlin'
            if timezone == 'EDT': timezone = 'America/New_York'
            if timezone == 'CDT': timezone = 'America/Chicago'
            # Convert the timezone information to UTC
            utc = pytz.timezone('UTC')
            local_tz = pytz.timezone(timezone)
            start_time = datetime.datetime.fromtimestamp(metadata['pre']['start'], local_tz)
            end_time = datetime.datetime.fromtimestamp(metadata['post']['end'], local_tz)
            return start_time, end_time
        except BaseException as e:
            return None,None
    started = time.time()
    updatetime = 0.5
    res = False
    olddate = None
    try:
        sym = connection.FindSymbol(paper,market)
        if sym == None or (not 'name' in paper) or paper['name'] == None or paper['name'] == paper['ticker']:
            res = None
            if 'isin' in paper and paper['isin']:
                res = await SearchPaper(paper['isin'])
            if not res and 'ticker' in paper and paper['ticker']:
                res = await SearchPaper(paper['ticker'])
            if res:
                paper['ticker'] = res['symbol']
                if 'longname' in res:
                    paper['name'] = res['longname']
                elif 'shortname' in res:
                    paper['name'] = res['shortname']
            else:
                logging.warning('paper '+paper['isin']+' not found !')
                return False,None
        if 'ticker' in paper and paper['ticker']:
            startdate = datetime.datetime.utcnow()-datetime.timedelta(days=365*3)
            if sym == None and res:
                #initial download
                markett = database.Market.stock
                if res['quoteType'] == 'INDEX':
                    markett = database.Market.index
                    paper['isin'] = paper['ticker']
                sym = database.Symbol(isin=paper['isin'],ticker=paper['ticker'],name=paper['name'],market=markett,active=True)
                try:
                    connection.session.add(sym)
                    connection.session.commit()
                except BaseException as e:
                    logging.warning('failed writing to db:'+str(e))
                    connection.session.rollback()
            sym = connection.FindSymbol(paper,market)
            if sym:
                date_entry,latest_date = connection.session.query(database.MinuteBar,sqlalchemy.sql.expression.func.max(database.MinuteBar.date)).filter_by(symbol=sym).first()
                startdate = latest_date
                if latest_date:
                    next_update = latest_date+datetime.timedelta(seconds=GetUpdateFrequency())
                    if (next_update-datetime.datetime.utcnow() < (datetime.timedelta(minutes=GetUpdateFrequency() / 4))):
                        await asyncio.sleep((next_update-datetime.datetime.utcnow()).total_seconds())
                    else: #when wait-time >90% return and wait for next cycle
                        return False,None
                if not startdate:
                    startdate = datetime.datetime.utcnow()-datetime.timedelta(days=59)
                try:
                    while startdate < datetime.datetime.utcnow():
                        from_timestamp = int((startdate - datetime.datetime(1970, 1, 1)).total_seconds())
                        to_timestamp = int(((startdate+datetime.timedelta(days=59)) - datetime.datetime(1970, 1, 1)).total_seconds())
                        if (not (sym.tradingstart and sym.tradingend))\
                        or (datetime.datetime.utcnow()-startdate>datetime.timedelta(days=0.8))\
                        or sym.tradingstart.time() <= datetime.datetime.utcnow().time() <= sym.tradingend.time():
                            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{paper['ticker']}?interval=15m&includePrePost=true&events=history&period1={from_timestamp}&period2={to_timestamp}"
                            async with aiohttp.ClientSession(headers={'User-Agent': UserAgent}) as session:
                                async with session.get(url) as resp:
                                    data = await resp.json()
                                    if data["chart"]["result"]:
                                        sym.tradingstart, sym.tradingend = extract_trading_times(data["chart"]["result"][0]['meta']['currentTradingPeriod'])
                                        sym.currency = data["chart"]["result"][0]['meta']['currency']
                                        ohlc_data = data["chart"]["result"][0]["indicators"]["quote"][0]
                                        if len(ohlc_data)>0:
                                            pdata = pandas.DataFrame({
                                                "Datetime": data["chart"]["result"][0]["timestamp"],
                                                "Open": ohlc_data["open"],
                                                "High": ohlc_data["high"],
                                                "Low": ohlc_data["low"],
                                                "Close": ohlc_data["close"],
                                                "Volume": ohlc_data["volume"]
                                            })
                                            pdata["Datetime"] = pandas.to_datetime(pdata["Datetime"], unit="s")
                                            pdata = pdata.dropna()
                                            if pdata["Datetime"].iloc[-1].minute % 15 != 0:
                                                # Entferne die letzte Zeile aus dem DataFrame
                                                pdata = pdata.iloc[:-1]
                                            try:
                                                olddate = sym.GetActDate(connection.session)
                                                connection.session.add(sym)
                                                acnt = sym.AppendData(connection.session,pdata)
                                                res = res or acnt>0
                                                connection.session.commit()
                                                if res: 
                                                    logging.info('yahoo:'+sym.ticker+' succesful updated '+str(acnt)+' till '+str(pdata['Datetime'].iloc[-1])+' ('+str(sym.tradingend)+')')
                                                    olddate = pdata['Datetime'].iloc[-1]
                                                else:
                                                    logging.info('yahoo:'+sym.ticker+' no new data')
                                                updatetime = 10
                                                #res = True
                                            except BaseException as e:
                                                logging.warning('failed writing to db:'+str(e))
                                                connection.session.rollback()
                                        else:
                                            logging.info('yahoo:'+sym.ticker+' no new data')
                        startdate += datetime.timedelta(days=59)
                except BaseException as e:
                    logging.error('failed updating ticker %s: %s' % (str(paper['isin']),str(e)))
    except BaseException as e:
        logging.error('failed updating ticker %s: %s' % (str(paper['isin']),str(e)))
    await asyncio.sleep(updatetime-(time.time()-started)) #3 times per minute
    return res,olddate
def GetUpdateFrequency():
    return 15*60
async def SearchPaper(isin):
    url = 'https://query1.finance.yahoo.com/v1/finance/search'
    headers = {
        'User-Agent': UserAgent,
    }
    params = dict(
        q=isin,
        quotesCount=1,
        newsCount=0,
        listsCount=0,
        quotesQueryId='tss_match_phrase_query'
    )
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as resp:
            data = await resp.json()
            if 'quotes' in data and len(data['quotes']) > 0:
                return data['quotes'][0]
    return None
class UpdateTickers(threading.Thread):
    def __init__(self, papers, market,name, delay=0) -> None:
        super().__init__(name='Ticker-Update yahoo-'+name)
        self.papers = papers
        self.market = market
        self.WaitTime = 60/3
        self.Delay = delay
        self.start()
    def run(self):
        self.loop = asyncio.new_event_loop()
        self.connection = database.Connection()
        while True:
            internal_updated = {}
            started = time.time()
            try:
                earliest = datetime.datetime.now()
                for paper in self.papers:
                    if internal_updated.get(paper['isin']) == None: 
                        epaper = paper
                        break
                    if internal_updated.get(paper['isin'])<earliest:
                        earliest = internal_updated.get(paper['isin'])
                        epaper = paper
                if not internal_updated.get(paper['isin']) or internal_updated.get(paper['isin']) < datetime.datetime.now()-datetime.timedelta(seconds=self.Delay):
                    res,till = self.loop.run_until_complete(UpdateTicker(epaper,self.market,self.connection))
                    if not till: till = datetime.datetime.now()
                    internal_updated[paper['isin']] = till
            except BaseException as e:
                logging.error(str(e))
            if self.WaitTime-(time.time()-started) > 0:
                time.sleep(self.WaitTime-(time.time()-started))
def StartUpdate(papers,market,name):
    return UpdateTickers(papers,market,name)
if __name__ == '__main__':
    logging.root.setLevel(logging.DEBUG)
    apaper = {
        "isin": "DE0007037129",
        "count": 0,
        "price": 0,
        "ticker": "RWE.DE",
        "name": "RWE Aktiengesellschaft"
    }
    apaper1 = {
        "isin": None,
        "count": 0,
        "price": 0,
        "ticker": "^TECDAX",
        "name": "Tech DAX"
    }
    StartUpdate([apaper,apaper1],'gettex').join()