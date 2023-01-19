import requests,yfinance,pandas,datetime,pathlib,database
def UpdateSettings(paper):
    #tf = yfinance.Ticker(paper['ticker'])
    #info = tf.info
    if not 'name' in paper:
        paper['name'] = paper['ticker']
    return paper
def UpdateTickers(papers):
    tickers = []
    for paper in papers:
        tickers.append(paper['ticker'])
    for paper in papers:
        if database.session.query(database.Symbol.isin).filter_by(isin=paper['isin']).first() is None:
            data = yfinance.download(tickers=paper['ticker'],period="2y",interval = "1h")
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
    for paper in papers:
        sym = database.session.execute(database.sqlalchemy.select(database.Symbol).where(database.Symbol.isin==paper['isin'])).fetchone()[0]
        #sym = database.session.query(database.Symbol.isin).filter_by(isin=paper['isin']).first()
        data = yfinance.download(tickers=paper['ticker'],period="1d",interval = "1h")
        data.reset_index(inplace=True)
        for row in range(len(data)):
            if database.session.query(database.MinuteBar.date,database.MinuteBar.symbol).filter_by(date=data['Datetime'].loc[row],symbol=sym).first() is None:
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
def GetActPrice(paper):
    file = Data / ('%s.pkl' % paper['isin'])
    data = pandas.read_pickle(str(file))
    return data['Close'].tail(1).values[0]
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