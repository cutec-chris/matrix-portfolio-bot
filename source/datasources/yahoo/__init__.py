import pathlib,sys;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
import asyncio,aiohttp,csv,datetime,pytz,time,threading,concurrent.futures
import requests,pandas,pathlib,database,sqlalchemy.sql.expression,asyncio,logging,io,random
logger = logging.getLogger('yahoo')
UserAgent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36'
async def DownloadChunc(session,sym,from_date,to_date,timeframe,paper,market):
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
    res = False
    olddate = None
    if market != None:
        return res,olddate
    from_timestamp = int((from_date - datetime.datetime(1970, 1, 1)).total_seconds())
    to_timestamp = int((to_date - datetime.datetime(1970, 1, 1)).total_seconds())
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{paper['ticker']}?interval={timeframe}&includePrePost=true&events=history&period1={from_timestamp}&period2={to_timestamp}"
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
                    pdata["Datetime"] = pdata["Datetime"].dt.floor('S')
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
                            logger.info(sym.ticker+' succesful updated '+str(acnt)+' till '+str(pdata['Datetime'].iloc[-1])+' from '+str(olddate))
                            olddate = pdata['Datetime'].iloc[-1]
                        else:
                            logger.info(sym.ticker+' no new data')
                        updatetime = 10
                    except BaseException as e:
                        logger.warning('failed writing to db:'+str(e))
                        connection.session.rollback()
                else:
                    logger.info(paper['ticker']+' no new data')
    return res,olddate
async def UpdateTicker(paper,market=None):
    return await database.UpdateTickerProto(paper,market,DownloadChunc,SearchPaper,30,730)
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
                res = data['quotes'][0]
                if res['quoteType'] == 'EQUITY':
                    res['type'] = 'stock'
            else: res['type'] = res['quoteType'].lower()
            return res
    return None
async def StartUpdate(papers,market,name):
    await database.UpdateCyclic(papers,market,name,UpdateTicker,15*60).run()
if __name__ == '__main__':
    logger.root.setLevel(logging.DEBUG)
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
    apaper2 = {
        "isin": "LU0252633754",
        "count": 0,
        "price": 0,
        "ticker": "LYY7.DE",
        "name": "LYY7.DE"
    }
    asyncio.run(StartUpdate([apaper2],None,''))