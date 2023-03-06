import asyncio,aiohttp,csv,datetime,pytz,time
import requests,pandas,pathlib,database,sqlalchemy.sql.expression,asyncio,logging,io
UserAgent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36'
async def UpdateTicker(paper):
    def extract_trading_times(metadata):
        try:
            timezone = metadata['regular']['timezone']
            # Convert the timezone information to UTC
            utc = pytz.timezone('UTC')
            local_tz = pytz.timezone(timezone)
            start_time = datetime.datetime.fromtimestamp(metadata['pre']['start'], local_tz)
            end_time = datetime.datetime.fromtimestamp(metadata['post']['end'], local_tz)
            return start_time, end_time
        except:
            return None,None
    started = time.time()
    updatetime = 0.5
    res = False
    try:
        sym = database.session.query(database.Symbol).filter_by(isin=paper['isin']).first()
        if sym == None or (not 'name' in paper) or paper['name'] == None or paper['name'] == paper['ticker']:
            res = await SearchPaper(paper['isin'])
            if res:
                paper['ticker'] = res['symbol']
                if 'longname' in res:
                    paper['name'] = res['longname']
                elif 'shortname' in res:
                    paper['name'] = res['shortname']
            else:
                logging.warning('paper '+paper['isin']+' not found !')
        if 'ticker' in paper and paper['ticker']:
            startdate = datetime.datetime.utcnow()-datetime.timedelta(days=365*3)
            if sym == None and res:
                #initial download
                sym = database.Symbol(isin=paper['isin'],ticker=paper['ticker'],name=paper['name'],market=database.Market.stock,active=True)
                try:
                    database.session.add(sym)
                    database.session.commit()
                except BaseException as e:
                    logging.warning('failed writing to db:'+str(e))
                    database.session.rollback()
            elif paper['_updated']:
                startdate = paper['_updated']
            sym = database.session.query(database.Symbol).filter_by(isin=paper['isin']).first()
            while startdate < datetime.datetime.utcnow():
                from_timestamp = int((startdate - datetime.datetime(1970, 1, 1)).total_seconds())
                to_timestamp = int(((startdate+datetime.timedelta(days=59)) - datetime.datetime(1970, 1, 1)).total_seconds())
                try:
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
                                        try:
                                            sym.AppendData(pdata)
                                            database.session.add(sym)
                                            database.session.commit()
                                            logging.info(sym.ticker+' succesful updated till '+str(pdata['Datetime'].iloc[-1])+' ('+str(sym.tradingend)+')')
                                            updatetime = 10
                                            res = True
                                        except BaseException as e:
                                            logging.warning('failed writing to db:'+str(e))
                                            database.session.rollback()
                    startdate += datetime.timedelta(days=59)
                except BaseException as e:
                    logging.error('failed updating ticker %s: %s' % (str(paper['isin']),str(e)))
                    await asyncio.sleep(10)
    except BaseException as e:
        logging.error('failed updating ticker %s: %s' % (str(paper['isin']),str(e)))
    await asyncio.sleep(updatetime-(time.time()-started)) #3 times per minute
    return res
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