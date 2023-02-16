import asyncio,aiohttp,csv,datetime
import requests,yfinance,pandas,pathlib,database,sqlalchemy.sql.expression,asyncio,logging,io
async def UpdateTickers(papers):
    tickers = []
    for paper in papers:
        sym = database.session.query(database.Symbol).filter_by(isin=paper['isin']).first()
        if sym == None or paper['name'] == None or paper['name'] == paper['ticker']:
            res = SearchPaper(paper['isin'])
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
                url = f"https://query1.finance.yahoo.com/v7/finance/download/{paper['ticker']}?period1={from_timestamp}&period2={to_timestamp}&interval=15m&events=history&includeAdjustedClose=true"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        data = await resp.text()
                        pdata = pandas.read_csv(io.StringIO(data))
                        sym.AppendData(pdata)
                database.session.add(sym)
                database.session.commit()
                startdate += datetime.timedelta(days=60)
        #15min update
        if paper['ticker']:
            sym = database.session.query(database.Symbol).filter_by(isin=paper['isin']).first()
            startdate = paper['updated']
            from_timestamp = int((startdate - datetime.datetime(1970, 1, 1)).total_seconds())
            to_timestamp = int(((startdate+datetime.timedelta(days=60)) - datetime.datetime(1970, 1, 1)).total_seconds())
            url = f"https://query1.finance.yahoo.com/v7/finance/download/{paper['ticker']}?period1={from_timestamp}&period2={to_timestamp}&interval=15m&events=history&includeAdjustedClose=true"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.text()
                    pdata = pandas.read_csv(io.StringIO(data))
                    sym.AppendData(pdata)
            database.session.add(sym)
            database.session.commit()
def GetUpdateFrequency():
    return 15*60
def SearchPaper(isin):
    url = 'https://query1.finance.yahoo.com/v1/finance/search'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
    }
    params = dict(
        q=isin,
        quotesCount=1,
        newsCount=0,
        listsCount=0,
        quotesQueryId='tss_match_phrase_query'
    )
    resp = requests.get(url=url, headers=headers, params=params)
    data = resp.json()
    if 'quotes' in data and len(data['quotes']) > 0:
        return data['quotes'][0]
    else:
        return None