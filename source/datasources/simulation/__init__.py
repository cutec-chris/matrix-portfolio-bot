import logging,database
async def UpdateTicker(paper,market=None):
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
        return True,None
    except BaseException as e:
        return False,None
def GetUpdateFrequency():
    return 1*60
