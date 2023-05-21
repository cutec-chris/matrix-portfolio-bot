import pathlib,sys;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
import sys,pathlib;sys.path.append(str(pathlib.Path(__file__).parent / 'pyonvista' / 'src'))
import pyonvista,asyncio,aiohttp,datetime,pytz,time,logging,database,pandas,json,aiofiles,datetime,sqlalchemy,threading,random
logger = logging.getLogger('onvista')
async def DownloadChunc(session,sym,from_date,to_date,timeframe,paper,market):
    client = aiohttp.ClientSession()
    res = False
    olddate = None
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
        enddate = to_date
        if enddate>datetime.datetime.utcnow():
            enddate = None
        quotes = await api.request_quotes(instrument,notation=t_market,start=from_date.date(),end=enddate)
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
            pdata["Datetime"] = pandas.to_datetime(pdata["Datetime"])
            gmtoffset = datetime.datetime.now()-datetime.datetime.utcnow()
            pdata["Datetime"] -= gmtoffset
            pdata["Datetime"] = pdata["Datetime"].dt.floor('S')
            try:
                olddate = await sym.GetActDate(session)
                acnt = await sym.AppendData(session,pdata)
                res = acnt>0
                session.add(sym)
                if res: 
                    logger.info(sym.ticker+' succesful updated '+str(acnt)+' till '+str(pdata['Datetime'].iloc[-1])+' from '+str(olddate))
                    olddate = pdata['Datetime'].iloc[-1]
                else:
                    logger.info(sym.ticker+' no new data')
                updatetime = 10
            except BaseException as e:
                logger.warning('failed writing to db:'+str(e))
        return res,olddate
async def UpdateTicker(paper,market=None):
    return await database.UpdateTickerProto(paper,market,DownloadChunc,SearchPaper)
async def SearchPaper(isin):
    client = aiohttp.ClientSession()
    try:
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
    except BaseException as e: pass
    return None
async def StartUpdate(papers,market,name):
    await database.UpdateCyclic(papers,market,name,UpdateTicker,15*60,60/12).run()
if __name__ == '__main__':
    logger.root.setLevel(logger.INFO)
    apaper = {
        "isin": "DE0007037129",
        "count": 0,
        "price": 0,
        "ticker": "RWE",
        "name": "RWE Aktiengesellschaft"
    }
    asyncio.run(StartUpdate([apaper],'gettex',''))