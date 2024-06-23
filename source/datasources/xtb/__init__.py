import os,asyncio,json,websockets,yaml,pathlib,logging,database
logger = logging.getLogger('xtb')
config = None
with open(pathlib.Path(__file__).parent / "config.yaml", "r") as file:
    config = yaml.safe_load(file)
Socket = None
StreamSessionID = None
async def UpdateTicker(paper,market=None):
    started = time.time()
    updatetime = 0.5
    res = False
    olddate = None
    async with database.new_session() as session,session.begin():
        try:
            sym = await database.FindSymbol(session,paper,None)
            if sym == None or (not 'name' in paper) or paper['name'] == None or paper['name'] == paper['ticker']:
                sres = None
                if 'isin' in paper and paper['isin']:
                    sres = await SearchPaper(paper['isin'])
                if not res and 'ticker' in paper and paper['ticker']:
                    sres = await SearchPaper(paper['ticker'])
                if sres:
                    paper['ticker'] = sres['symbol']
                    if 'longname' in sres:
                        paper['name'] = sres['longname']
                    elif 'shortname' in sres:
                        paper['name'] = sres['shortname']
                else:
                    logger.warning('paper '+paper['isin']+' not found !')
                    return False,None
            if 'ticker' in paper and paper['ticker']:
                startdate = datetime.datetime.utcnow()-datetime.timedelta(days=365*3)
                if sym == None and sres:
                    #initial download
                    markett = database.Market.stock
                    if sres['quoteType'] == 'INDEX':
                        markett = database.Market.index
                        paper['isin'] = paper['ticker']
                    sym = database.Symbol(isin=paper['isin'],ticker=paper['ticker'],name=paper['name'],market=markett,active=True)
                    try:
                        session.add(sym)
                    except BaseException as e:
                        logger.warning('failed writing to db:'+str(e))
                if sym:
                    result = await session.execute(sqlalchemy.select(database.MinuteBar, sqlalchemy.func.max(database.MinuteBar.date)).where(database.MinuteBar.symbol == sym))
                    date_entry, latest_date = result.fetchone()
                    startdate = latest_date
                    if not startdate:
                        startdate = datetime.datetime.utcnow()-datetime.timedelta(days=59)
                    try:
                        while startdate < datetime.datetime.utcnow():
                            from_timestamp = int((startdate - datetime.datetime(1970, 1, 1)).total_seconds())
                            to_timestamp = int(((startdate+datetime.timedelta(days=59)) - datetime.datetime(1970, 1, 1)).total_seconds())
                            if (not (sym.tradingstart and sym.tradingend))\
                            or ((sym.tradingstart.time() <= datetime.datetime.utcnow().time() <= sym.tradingend.time()) and (datetime.datetime.utcnow().weekday() < 5)):
                                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{paper['ticker']}?interval=15m&includePrePost=true&events=history&period1={from_timestamp}&period2={to_timestamp}"
                                async with aiohttp.ClientSession(headers={'User-Agent': UserAgent}) as hsession:
                                    async with hsession.get(url) as resp:
                                        data = await resp.json()
                                        if data["chart"]["result"]:
                                            if not sym.tradingstart or not sym.currency:
                                                sym.tradingstart, sym.tradingend = extract_trading_times(data["chart"]["result"][0]['meta']['currentTradingPeriod'])
                                                sym.currency = data["chart"]["result"][0]['meta']['currency']
                                            gmtoffset_timedelta = datetime.timedelta(seconds=data["chart"]["result"][0]['meta']['gmtoffset'])
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
                                                #pdata["Datetime"] -= gmtoffset_timedelta
                                                pdata["Datetime"] = pdata["Datetime"].dt.floor('s')
                                                pdata = pdata.dropna()
                                                if pdata["Datetime"].iloc[-1].minute % 15 != 0:
                                                    # Entferne die letzte Zeile aus dem DataFrame
                                                    pdata = pdata.iloc[:-1]
                                                try:
                                                    olddate = await sym.GetActDate(session)
                                                    session.add(sym)
                                                    acnt = await sym.AppendData(session,pdata)
                                                    res = res or acnt>0
                                                    if res: 
                                                        logger.info('yahoo:'+sym.ticker+' succesful updated '+str(acnt)+' till '+str(pdata['Datetime'].iloc[-1])+' from '+str(olddate))
                                                        olddate = pdata['Datetime'].iloc[-1]
                                                    else:
                                                        logger.info('yahoo:'+sym.ticker+' no new data')
                                                    updatetime = 10
                                                except BaseException as e:
                                                    logger.warning('failed writing to db:'+str(e))
                                                    connection.session.rollback()
                                            else:
                                                logger.info('yahoo:'+paper['ticker']+' no new data')
                            startdate += datetime.timedelta(days=59)
                        if res: await session.commit()
                    except BaseException as e:
                        logger.error('failed updating ticker %s: %s' % (str(paper['isin']),str(e)))
        except BaseException as e:
            logger.error('failed updating ticker %s: %s' % (str(paper['isin']),str(e)))
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
async def DoConnect():
    Socket = websockets.connect(config['xtb']['base_url'])
    # Authentifiziere und abonniere für Minute Bars
    logger.info('sucessfully connected')
    auth_data = {
        "command": "login",
        "arguments": {
            "userId": config['xtb']['user'],
            "password": config['xtb']['password'],
            "appName": "Portfolio-Managment"
        }
    }
    await Socket.send(json.dumps(auth_data))
    auth_response = await Socket.recv()
    auth_response_json = json.loads(auth_response)
    auth_success = False
    if auth_response_json.get("status") == True:
        StreamSessionID = auth_response_json.get("streamSessionId")
        auth_success = True
    if not auth_success:
        return False
    return True
    """
    logger.info('sucessfully authentificated')
    async with websockets.connect(config['xtb']['base_url']+'Stream') as websocket_s:
        logger.info('connected to streaming server')
        await websocket_s.send(json.dumps({
            "command": "getNews",
            "streamSessionId": StreamSessionID,
        }))
        for ticker in tickers:
            candle_data = {
                "command": "getCandles",
                "streamSessionId": StreamSessionID,
                "symbol": ticker,
            }
            await websocket_s.send(json.dumps(candle_data))
            await asyncio.sleep(0.2)
        logger.info('%d symbols subscribed' % len(tickers))
        await websocket_s.send(json.dumps({
            "command": "getBalance",
            "streamSessionId": StreamSessionID,
        }))
        await websocket_s.send(json.dumps({
            "command": "getTrades",
            "streamSessionId": StreamSessionID,
        }))
        
        #await websocket_s.send(json.dumps({
        #    "command": "getKeepAlive",
        #    "streamSessionId": StreamSessionID,
        #}))
        while True:
            response = await websocket_s.recv()
            logger.info(f"< {response}")
    """
async def StartUpdate(papers, market, name):
    if config:
        if await DoConnect():
            await database.UpdateCyclic(papers,market,name,UpdateTicker,15*60).run()
if __name__ == '__main__':
    logger.root.setLevel(logger.INFO)
    papers = [
        {
            "isin": None,
            "ticker": "BITCOIN",
        },
        {
            "isin": None,
            "ticker": "ETHERIUM",
        },
        {
            "isin": None,
            "ticker": "SOLANA",
        },
        {
            "isin": None,
            "ticker": "EURUSD",
        },
        {
            "isin": None,
            "ticker": "UNISWAP",
        }
    ]
    asyncio.run(StartUpdate(papers, "market", "name"))