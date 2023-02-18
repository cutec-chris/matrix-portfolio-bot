import asyncio,aiohttp,csv,datetime
import requests,yfinance,pandas,pathlib,database,sqlalchemy.sql.expression,asyncio,logging,io
UserAgent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36'
async def UpdateTickers(papers):
    tickers = []
    for paper in papers:
        try:
            sym = database.session.query(database.Symbol).filter_by(isin=paper['isin']).first()
            if sym == None or paper['name'] == None or paper['name'] == paper['ticker']:
                res = await SearchPaper(paper['isin'])
                if res:
                    paper['ticker'] = res['symbol']
                    paper['name'] = res['longname']
            if sym == None:
                #initial download
                sym = database.Symbol(isin=paper['isin'],ticker=paper['ticker'],name=paper['name'],market=database.Market.stock,active=True)
                startdate = datetime.datetime.utcnow()-datetime.timedelta(days=365*3)
                while startdate < datetime.datetime.utcnow():
                    from_timestamp = int((startdate - datetime.datetime(1970, 1, 1)).total_seconds())
                    to_timestamp = int(((startdate+datetime.timedelta(days=60)) - datetime.datetime(1970, 1, 1)).total_seconds())
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{paper['ticker']}?interval=15m&includePrePost=false&events=history&period1={from_timestamp}&period2={to_timestamp}"
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url) as resp:
                            data = await resp.json()
                            ohlc_data = data["chart"]["result"][0]["indicators"]["quote"][0]
                            pdata = pandas.DataFrame({
                                "Datetime": data["chart"]["result"][0]["timestamp"],
                                "Open": ohlc_data["open"],
                                "High": ohlc_data["high"],
                                "Low": ohlc_data["low"],
                                "Close": ohlc_data["close"],
                                "Volume": ohlc_data["volume"]
                            })
                            pdata["Datetime"] = pandas.to_datetime(pdata["Datetime"], unit="s")
                            sym.AppendData(pdata)
                    database.session.add(sym)
                    try:
                        database.session.commit()
                    except BaseException as e:
                        logging.warning('failed updating ticker:'+str(e))
                        database.session.rollback()
                    startdate += datetime.timedelta(days=60)
            #15min update
            if paper['ticker']:
                sym = database.session.query(database.Symbol).filter_by(isin=paper['isin']).first()
                startdate = paper['updated']
                from_timestamp = int((startdate - datetime.datetime(1970, 1, 1)).total_seconds())
                to_timestamp = int(((startdate+datetime.timedelta(days=60)) - datetime.datetime(1970, 1, 1)).total_seconds())
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{paper['ticker']}?interval=15m&includePrePost=false&events=history&period1={from_timestamp}&period2={to_timestamp}"
                async with aiohttp.ClientSession(headers={'User-Agent': UserAgent}) as session:
                    async with session.get(url) as resp:
                        data = await resp.json()
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
                            pdata.dropna()
                            sym.AppendData(pdata)
                database.session.add(sym)
                try:
                    database.session.commit()
                except BaseException as e:
                    logging.warning('failed updating ticker:'+str(e))
                    database.session.rollback()
        except BaseException as e:
            logging.error('failed updating ticker %s: %s' % (paper['ticker'],str(e)))
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