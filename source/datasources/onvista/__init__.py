import sys,pathlib;sys.path.append(str(pathlib.Path(__file__).parent / 'pyonvista' / 'src'))
import pyonvista,asyncio,aiohttp,datetime,pytz,time,logging,database,pandas
async def UpdateTicker(paper,market=None):
    client = aiohttp.ClientSession()
    api = pyonvista.PyOnVista()
    await api.install_client(client)
    started = time.time()
    updatetime = 0.5
    res = False
    try:
        sym = database.session.query(database.Symbol).filter_by(isin=paper['isin'],marketplace=market).first()
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
            startdate = datetime.datetime.utcnow()-datetime.timedelta(days=30)
            if sym == None and res:
                #initial download
                sym = database.Symbol(isin=paper['isin'],ticker=paper['ticker'],name=paper['name'],market=database.Market.stock,marketplace=market,active=True)
                if market == 'gettex':
                    sym.tradingstart = datetime.datetime.now().replace(hour=7,minute=0)
                    sym.tradingend = datetime.datetime.now().replace(hour=21,minute=0)
                try:
                    database.session.add(sym)
                    database.session.commit()
                except BaseException as e:
                    logging.warning('failed writing to db:'+str(e))
                    database.session.rollback()
            elif paper['_updated']:
                startdate = paper['_updated']
            sym = database.session.query(database.Symbol).filter_by(isin=paper['isin'],marketplace=market).first()
            async with client:
                i = pyonvista.api.Instrument()
                i.type=str(sym.market).upper()[7:]
                i.isin = paper['isin']
                i.notations = []
                instrument = await api.request_instrument(isin=paper['isin'],instrument=i)
                t_market = None
                if market:
                    for m in instrument.notations:
                        if m.market.name == market:
                            t_market = m
                            break
                while startdate < datetime.datetime.utcnow():
                    quotes = await api.request_quotes(instrument,notation=t_market,start=startdate,end=startdate+datetime.timedelta(days=7))
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
                        # Erstellen des DataFrames aus der Liste von Dictionaries
                        df = pandas.DataFrame(data)
                        pdata = df.dropna()
                        try:
                            sym.AppendData(pdata)
                            database.session.add(sym)
                            database.session.commit()
                            logging.info(sym.ticker+' succesful updated till '+str(pdata['Datetime'].iloc[-1])+' ('+str(sym.tradingend)+')')
                            updatetime = 20
                            res = True
                        except BaseException as e:
                            logging.warning('failed writing to db:'+str(e))
                            database.session.rollback()
                    await asyncio.sleep(1)
                    startdate += datetime.timedelta(days=7)
    except BaseException as e:
        logging.error('failed updating ticker %s: %s' % (str(paper['isin']),str(e)))
        await asyncio.sleep(10)
def GetUpdateFrequency():
    return 15*60
async def SearchPaper(isin):
    client = aiohttp.ClientSession()
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
    return None