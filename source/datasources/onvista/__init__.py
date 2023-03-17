import sys,pathlib;sys.path.append(str(pathlib.Path(__file__).parent / 'pyonvista' / 'src'))
import pyonvista,asyncio,aiohttp,datetime,pytz,time,logging,database,pandas,json,aiofiles,datetime,sqlalchemy
async def UpdateTicker(paper,market=None):
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
                return False
        if 'ticker' in paper and paper['ticker']:
            startdate = datetime.datetime.utcnow()-datetime.timedelta(days=30)
            if sym == None and res:
                #initial download
                sym = database.Symbol(isin=paper['isin'],ticker=paper['ticker'],name=paper['name'],market=database.Market[res['type'].lower()],marketplace=market,active=True)
                if market == 'gettex':
                    sym.tradingstart = datetime.datetime.now().replace(hour=7,minute=0)
                    sym.tradingend = datetime.datetime.now().replace(hour=21,minute=0)
                try:
                    database.session.add(sym)
                    database.session.commit()
                except BaseException as e:
                    logging.warning('failed writing to db:'+str(e))
                    database.session.rollback()
            sym = database.session.query(database.Symbol).filter_by(isin=paper['isin'],marketplace=market).first()
            if sym:
                date_entry,latest_date = database.session.query(database.MinuteBar,sqlalchemy.sql.expression.func.max(database.MinuteBar.date)).filter_by(symbol=sym).first()
                startdate = latest_date
                if latest_date:
                    next_update = latest_date+datetime.timedelta(seconds=GetUpdateFrequency())
                    if datetime.datetime.utcnow()<next_update:
                        if (next_update-datetime.datetime.utcnow() < (datetime.timedelta(minutes=GetUpdateFrequency() / 4))):
                            await asyncio.sleep((next_update-datetime.datetime.utcnow()).total_seconds())
                        else: #when wait-time >90% return and wait for next cycle
                            return False
                if (not (sym.tradingstart and sym.tradingend))\
                or (datetime.datetime.utcnow()-startdate>datetime.timedelta(days=0.8))\
                or sym.tradingstart.time() <= datetime.datetime.utcnow().time() <= sym.tradingend.time():
                    client = aiohttp.ClientSession()
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
                                return False
                        t_market = None
                        if market:
                            for m in instrument.notations:
                                if m.market.name == market:
                                    t_market = m
                                    break
                        while startdate.date() <= datetime.datetime.utcnow().date():
                            todate = startdate+datetime.timedelta(days=7)
                            if todate>datetime.datetime.now():
                                todate = None
                            quotes = await api.request_quotes(instrument,notation=t_market,start=startdate,end=todate)
                            if (pathlib.Path(__file__).parent / 'debug').exists():
                                async with aiofiles.open(str(pathlib.Path(__file__).parent / 'debug' / (paper['isin']+'_'+datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')+'.json')), 'w') as file:
                                    for quote in quotes:
                                        await file.write(str(quote)+'\n')
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
                                    acnt = sym.AppendData(pdata)
                                    res = res or acnt>0
                                    database.session.add(sym)
                                    database.session.commit()
                                    if res: 
                                        logging.info('onvista:'+sym.ticker+' succesful updated '+str(acnt)+' till '+str(pdata['Datetime'].iloc[-1])+' ('+str(sym.tradingend)+')')
                                    else:
                                        logging.info('onvista:'+sym.ticker+' no new data')
                                    updatetime = 10
                                except BaseException as e:
                                    logging.warning('failed writing to db:'+str(e))
                                    database.session.rollback()
                            await asyncio.sleep(1)
                            startdate += datetime.timedelta(days=7)
    except BaseException as e:
        logging.error('onvista:'+'failed updating ticker %s: %s' % (str(paper['isin']),str(e)), exc_info=True)
    return res
def GetUpdateFrequency():
    return 16*60
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