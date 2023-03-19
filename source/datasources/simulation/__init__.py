import logging,database,datetime,time
start_at = datetime.datetime(2023,2,1,8,0)
async def UpdateTicker(paper,market=None):
    global start_at
    started = time.time()
    updatetime = 0.5
    res = False
    try:
        sym = database.session.query(database.Symbol).filter_by(isin=paper['isin'],marketplace=market).first()
        if sym == None:
            logging.warning('simulation:symbol not found:'+paper['isin'])
            return False,None
        if (not 'name' in paper) or paper['name'] == None or paper['name'] == paper['ticker']:
            paper['ticker'] = sym.ticker
            paper['name'] = sym.name
        start_at += datetime.timedelta(minutes=15)
        logging.info('simulation: UpdateTicker returns '+str(start_at))
        return True,start_at
    except BaseException as e:
        return False,None
def GetUpdateFrequency():
    return 10
