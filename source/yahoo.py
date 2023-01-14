import requests,yfinance,pandas,datetime
def UpdateSettings(paper):
    #tf = yfinance.Ticker(paper['ticker'])
    #info = tf.info
    if not 'name' in paper:
        paper['name'] = paper['ticker']
    return paper
def UpdateCSV(ticker,file):
    tf = yfinance.Ticker(ticker)
    if not file.exists():
        data = yfinance.download(tickers=ticker,period="10y",interval = "1d")
        data.reset_index(inplace=True)
        data['Date'] = pandas.to_datetime(data['Date'])
        data = data.rename(columns={'Date': 'Datetime'})
        data.to_pickle(str(file))
    data = pandas.read_pickle(str(file))
    datam = yfinance.download(tickers=ticker,start=data['Datetime'].tail(1).values[0].astype(datetime.datetime).strftime('%Y-%m-%d'),interval = "1m")
    datam.reset_index(inplace=True)
    datam['Datetime'] = pandas.to_datetime(data['Datetime'])
    data = pandas.concat([data,datam])
    data.to_pickle(str(file))
    pass
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