import requests,yfinance,pandas,datetime,pathlib,database,sqlalchemy.sql.expression,asyncio
def UpdateSettings(paper):
    #tf = yfinance.Ticker(paper['ticker'])
    #info = tf.info
    if not 'name' in paper:
        paper['name'] = paper['ticker']
    return paper
async def UpdateTickers(papers):
    tickers = []
    for paper in papers:
        tickers.append(paper['ticker'])
    for paper in papers:
        if database.session.query(database.Symbol.isin).filter_by(isin=paper['isin']).first() is None and paper['ticker']:
            startdate = datetime.datetime.utcnow()-datetime.timedelta(days=365*3)
            while startdate < datetime.datetime.utcnow():
                data = yfinance.download(tickers=paper['ticker'],period="60d",interval = "15m")
                if not 'name' in paper:
                    paper['name'] = paper['ticker']
                sym = database.Symbol(isin=paper['isin'],ticker=paper['ticker'],name=paper['name'],market=database.Market.stock,active=True)
                data.reset_index(inplace=True)
                for row in range(len(data)):
                    sym.minute_bars.append(database.MinuteBar( 
                                        date=data['Datetime'].loc[row],
                                        open=data['Open'].loc[row],
                                        high=data['High'].loc[row],
                                        low=data['Low'].loc[row],
                                        close=data['Close'].loc[row],
                                        volume=data['Volume'].loc[row],
                                        symbol=sym,
                                    ))
                database.session.add(sym)
                database.session.commit()
                startdate += datetime.timedelta(days=60)
    for paper in papers:
        if paper['ticker']:
            sym = database.session.execute(database.sqlalchemy.select(database.Symbol).where(database.Symbol.isin==paper['isin'])).fetchone()[0]
            date_entry,latest_date = database.session.query(database.MinuteBar,sqlalchemy.sql.expression.func.max(database.MinuteBar.date)).filter_by(symbol=sym).first()
            data = yfinance.download(tickers=paper['ticker'],start=latest_date,period="5d",interval = "15m")
            data.reset_index(inplace=True)
            if len(data)>0:
                await asyncio.sleep(5)
            for row in range(len(data)):
                await asyncio.sleep(0.1)
                oldrow = database.session.query(database.MinuteBar).filter_by(date=data['Datetime'].loc[row],symbol=sym).first()
                if oldrow is None:
                    sym.minute_bars.append(database.MinuteBar( 
                                        date=data['Datetime'].loc[row],
                                        open=data['Open'].loc[row],
                                        high=data['High'].loc[row],
                                        low=data['Low'].loc[row],
                                        close=data['Close'].loc[row],
                                        volume=data['Volume'].loc[row],
                                        symbol=sym,
                                    ))
            database.session.add(sym)
            database.session.commit()
def get_symbol_for_isin(isin):
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
        return data['quotes'][0]['symbol']
    else:
        return None