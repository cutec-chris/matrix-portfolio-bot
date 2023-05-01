from init import *
import pathlib,database,pandas_ta,importlib.util,logging,os,pandas,sqlalchemy.sql.expression,datetime,sys,backtrader,time,aiofiles,random,backtests,os
import managepaper,processpaper
loop = None
lastsend = None
class Portfolio(Config):
    def __init__(self, room, **kwargs) -> None:
        super().__init__(room, **kwargs)
@bot.listener.on_message_event
async def tell(room, message):
    try:
        global servers,lastsend
        logging.info(str(message))
        if not message.body.startswith(prefix) and room.member_count==2:
            message.body = prefix+' '+message.body
        match = botlib.MessageMatch(room, message, bot, prefix)
        if (match.is_not_from_this_bot() and match.prefix()):
            res = await bot.api.async_client.room_typing(room.room_id,True,timeout=30000)
        if (match.is_not_from_this_bot() and match.prefix())\
        and (match.command("buy",case_sensitive=False)\
        or match.command("sell",case_sensitive=False)\
        or match.command("remove",case_sensitive=False)\
        or match.command("add",case_sensitive=False)):
            return await managepaper.manage_paper(room,message,match)
        elif (match.is_not_from_this_bot() and match.prefix())\
        and match.command("analyze",case_sensitive=False):
            return await processpaper.analyze(room,message,match)
        elif (match.is_not_from_this_bot() and match.prefix())\
        and match.command("show",case_sensitive=False):
            return await managepaper.show(room,message,match)
        elif (match.is_not_from_this_bot() and match.prefix())\
        and match.command("overview",case_sensitive=False):
            return await processpaper.overview(room,message,match)
        elif (match.is_not_from_this_bot() and match.prefix())\
        and match.command("create-depot"):
            pf = None
            for server in servers:
                if server.room == room.room_id and server.name == match.args()[1]:
                    pf = server
            if not pf:
                pf = Portfolio({
                    'room': room.room_id,
                    'name': match.args()[1],
                    'taxCostPercent': 0.0,
                    'tradingCost': 0.0,
                    'tradingCostPercent': 0.0,
                    'currency': 'EUR',
                    'papers': []
                })
            if len(match.args())>5:
                pf.currency = match.args()[5]
            if len(match.args())>4:
                pf.tradingCostPercent = float(match.args()[4])
            if len(match.args())>3:
                pf.tradingCost = float(match.args()[3])
            if len(match.args())>2:
                pf.taxCostPercent = float(match.args()[2])
            servers.append(pf)
            loop.create_task(processpaper.check_depot(pf),name='check-depot-'+pf.name)
            await save_servers()
            await bot.api.send_text_message(room.room_id, 'ok')
        elif (match.is_not_from_this_bot() and match.prefix())\
        and match.command("change-setting"):
            set_target = None
            for server in servers:
                if server.room == room.room_id and server.name == match.args()[1]:
                    setattr(server,match.args()[2],match.args()[3])
                    set_target = server
                    break
                for apaper in server.papers:
                    if apaper['isin'] == match.args()[1]:
                        apaper[match.args()[2]] = match.args()[3]
                        set_target = apaper
                        break
            if set_target:
                await save_servers()
                await bot.api.send_text_message(room.room_id, 'ok')
    except BaseException as e:
        logging.error(str(e), exc_info=True)
        await bot.api.send_text_message(room,str(e))
    await bot.api.async_client.room_typing(room.room_id,False,0)
async def ProcessStrategy(paper,depot,data):
    cerebro = None
    strategy = 'sma'
    if 'strategy' in paper:
        strategy = paper['strategy']
    elif hasattr(depot,'strategy'):
        strategy = depot.strategy
    for st in strategies:
        if st['name'] == strategy:
            cerebro = database.BotCerebro()
            cerebro.broker.setcash(1000)
            cerebro.addsizer(backtrader.sizers.PercentSizer, percents=100)
            cerebro.addstrategy(st['mod'].Strategy)
            break
    if cerebro and isinstance(data, pandas.DataFrame) and cerebro and not data.empty:
        logging.info(str(depot.name)+': processing ticker '+paper['ticker']+' till '+str(data.index[-1]))
        try:
            def run_cerebro():
                cerebro.adddata(backtrader.feeds.PandasData(dataname=data))
                cerebro.run()
            await asyncio.get_event_loop().run_in_executor(None, run_cerebro)
        except BaseException as e:
            logging.error(str(e))
            return False
        size_sum = 0
        price_sum = 0
        checkfrom = datetime.datetime.utcnow()-datetime.timedelta(days=30*3)
        if 'lastcheck' in paper: checkfrom = datetime.datetime.strptime(paper['lastcheck'], "%Y-%m-%d %H:%M:%S")
        orderdate = datetime.datetime.now()
        for order in cerebro._broker.orders:
            if order.status == 4:
                if order.executed.dt: orderdate = backtrader.num2date(order.executed.dt)
                if orderdate > checkfrom:
                    size_sum = order.size
                    #print(order.isbuy(),order.size,orderdate)
        if size_sum != 0:
            if not 'lastreco' in paper: paper['lastreco'] = ''
            if size_sum > 0:
                msg1 = 'strategy %s propose buying %d x %s %s (%s) at %s' % (strategy,round(size_sum),paper['isin'],paper['name'],paper['ticker'],orderdate)
                if hasattr(order,'chance'):
                    msg1 += ' chance %.1f till %s' % (order.chance,oder.chancetarget)
                msg1 += '\n'
                msg2 = 'buy %s %d' % (paper['isin'],round(size_sum))
                if paper['count']>0: return False
            else:
                msg1 = 'strategy %s propose selling %d x %s %s (%s) at %s' % (strategy,round(-size_sum),paper['isin'],paper['name'],paper['ticker'],orderdate)
                msg2 = 'sell %s %d' % (paper['isin'],round(-size_sum))
                if paper['count']==0: return False
            if strategy+':'+msg2 != paper['lastreco']:
                await bot.api.send_text_message(depot.room,msg1)
                await bot.api.send_text_message(depot.room,msg2)
                paper['lastreco'] = strategy+':'+msg2
                paper['lastcheck'] = orderdate.strftime("%Y-%m-%d %H:%M:%S")
                return True
    return False
async def ProcessIndicator(paper,depot,data):
    res = False
    def heikin_ashi(data):
        ha_data = pandas.DataFrame(index=data.index)
        ha_data['HA_Open'] = (data['Open'] + data['Close']) / 2
        ha_data['HA_High'] = data[['High', 'Open', 'Close']].max(axis=1)
        ha_data['HA_Low'] = data[['Low', 'Open', 'Close']].min(axis=1)
        ha_data['HA_Close'] = (data['Open'] + data['High'] + data['Low'] + data['Close']) / 4
        return ha_data
    hdata = data.resample('4H').agg({
                            'Open':'first',
                            'High':'max',
                            'Low':'min',
                            'Close':'last'})
    data = heikin_ashi(hdata)
    if   data.iloc[-2]['HA_Close']>data.iloc[-2]['HA_Open']:# and data.iloc[-2]['HA_Low']==data.iloc[-2]['HA_Open']:
        act_indicator = True
    elif data.iloc[-2]['HA_Close']<data.iloc[-2]['HA_Open']:# and data.iloc[-2]['HA_High']==data.iloc[-2]['HA_Close']:
        act_indicator = False
    else:
        act_indicator = None
    if not act_indicator: trend_symbol = '⌄'
    else: trend_symbol = '⌃'
    msg1 = 'trend changes to %s for %s %s (%s)' % (trend_symbol,paper['isin'],paper['name'],paper['ticker'])
    if 'trend_up' in paper:
        if act_indicator != paper['trend_up']:
            if act_indicator==False and paper['count']>0: #Downward Trend on an paper we have
                await bot.api.send_text_message(depot.room,msg1)
            elif act_indicator==True: #Trend changes to upwards
                await bot.api.send_text_message(depot.room,msg1)
            res = True
    else: res = True
    paper['trend_up'] = act_indicator
    return res
async def ChangeDepotStatus(depot,newstatus):
    global servers
    if newstatus!='':
        await bot.api.async_client.set_presence('online',newstatus)
    else:
        await bot.api.async_client.set_presence('unavailable','')
    ntext = ''
    i=0
    for adepot in servers:
        if adepot.room == depot.room:
            sumactprice = 0
            sumprice = 0
            for paper in adepot.papers:
                if paper['count'] > 0:
                    sym = database.session.query(database.Symbol).filter_by(isin=paper['isin'],marketplace=depot.market).first()
                    if sym:
                        actprice = sym.GetActPrice(depot.currency)
                    else: 
                        actprice = 0
                    sumactprice += actprice*paper['count']
                    sumprice += paper['price']
                    change = (actprice*paper['count'])-paper['price']
            ntext += '%s (%.2f)\n' % (adepot.name,sumactprice)
            i+=1
    room = bot.api.async_client.rooms.get(depot.room)
    #if ntext != room.topic:
    #    res = await bot.api.async_client.room_put_state(depot.room,'m.room.topic',{'topic': ntext},'')
    #    room.topic = ntext
async def check_depot(depot,fast=False):
    global lastsend,servers
    while True:
        updatedcurrencys = []
        check_status = []
        async def checkdatasource(datasource):
            started = time.time()
            ShouldSave = False
            FailedTasks = 0
            #if hasattr(depot,'datasource') and depot.datasource != datasource['name']: return
            UpdateTime = datasource['mod'].GetUpdateFrequency()
            logging.info(depot.name+' starting updates for '+datasource['name'])
            check_status.append(datasource['name'])
            await ChangeDepotStatus(depot,'updating '+" ".join(check_status))
            shuffled_papers = list(depot.papers)
            random.shuffle(shuffled_papers)
            for paper in shuffled_papers:
                targetmarket = None
                if hasattr(depot,'market'): targetmarket = depot.market
                if hasattr(datasource['mod'],'UpdateTicker'):
                    UpdateOK,TillUpdated = await datasource['mod'].UpdateTicker(paper,targetmarket)
                    if UpdateOK\
                    and hasattr(depot,'datasource') and depot.datasource == datasource['name']:
                        try:
                            currencypaper = None
                            sym = database.session.query(database.Symbol).filter_by(isin=paper['isin'],marketplace=targetmarket).first()
                            #Update also Currencys when currency is not depot cur
                            if sym and sym.currency and sym.currency != depot.currency and not sym.currency in updatedcurrencys:
                                currencypaper = {
                                    'isin': '%s%s=X' % (depot.currency,sym.currency),
                                    'ticker': '%s%s=X' % (depot.currency,sym.currency),
                                    'name': '%s/%s' % (depot.currency,sym.currency),
                                    '_updated': sym.GetActDate()
                                }
                                await datasource['mod'].UpdateTicker(currencypaper)
                                updatedcurrencys.append(sym.currency)
                            #Process strategy
                            if 'ticker' in paper and sym:
                                if sym:
                                    if sym.currency and sym.currency != depot.currency:
                                        df = sym.GetConvertedData((TillUpdated or datetime.datetime.utcnow())-datetime.timedelta(days=30*3),TillUpdated,depot.currency)
                                    else:
                                        df = sym.GetData((TillUpdated or datetime.datetime.utcnow())-datetime.timedelta(days=30*3),TillUpdated)
                                    ps = await run_in_thread(ProcessStrategy(paper,depot,df))
                                    ShouldSave = ShouldSave or ps
                                    #ps = await ProcessIndicator(paper,depot,df)
                                    #ShouldSave = ShouldSave or ps
                            FailedTasks = 0
                        except BaseException as e:
                            logging.error(str(e), exc_info=True)
                    elif not UpdateOK:
                        FailedTasks += 1
                    if FailedTasks > 3:
                        break
                if hasattr(datasource['mod'],'UpdateAnalystRatings'):
                    UpdateOK,TillUpdated = await datasource['mod'].UpdateAnalystRatings(paper,targetmarket)
                    ShouldSave |= UpdateOK
                if hasattr(datasource['mod'],'UpdateEarningsCalendar'):
                    UpdateOK,TillUpdated = await datasource['mod'].UpdateEarningsCalendar(paper,targetmarket)
                    ShouldSave |= UpdateOK
            logging.info(depot.name+' finished updates for '+datasource['name'])
            check_status.remove(datasource['name'])
            if check_status == []:#when we only await UpdateTime we can set Status
                await ChangeDepotStatus(depot,'')
            else:
                await ChangeDepotStatus(depot,'updating '+" ".join(check_status))
            if ShouldSave: 
                await save_servers()
            #Wait minimal one cyclus for the datasource
            await asyncio.sleep(UpdateTime-(time.time()-started))
        datasourcetasks = [asyncio.create_task(checkdatasource(datasource)) for datasource in datasources]
        await asyncio.wait(datasourcetasks)
datasources = []
strategies = []
connection = None
try:
    logging.basicConfig(level=logging.INFO)
    logging.info('starting event loop...')
    loop = asyncio.new_event_loop()
    logging.info('loading config...')
    with open('data.json', 'r') as f:
        nservers = json.load(f)
        for server in nservers:
            if not 'papers' in server:
                server['papers'] = []
            servers.append(Portfolio(server))
    logging.info('loading (and starting) datasources...')
    for folder in (pathlib.Path(__file__).parent / 'datasources').glob('*'):
        try:
            spec = importlib.util.spec_from_file_location(folder.name, str(folder / '__init__.py'))
            mod_ = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod_)
            module = {
                    'name': folder.name,
                    'mod': mod_      
                }
            datasources.append(module)
        except BaseException as e:
            logging.error(folder.name+':Failed to import datasource:'+str(e))
    logging.info('loading strategys...')
    for folder in (pathlib.Path(__file__).parent / 'strategy').glob('*/*.py'):
        try:
            spec = importlib.util.spec_from_file_location(folder.name, str(folder))
            mod_ = importlib.util.module_from_spec(spec)
            sys.modules[folder.name] = mod_
            spec.loader.exec_module(mod_)
            module = {
                    'name': folder.name.replace('.py',''),
                    'mod': mod_        
                }
            strategies.append(module)
        except BaseException as e:
            logging.error('Failed to import strategy:'+str(e))
except BaseException as e:
    logging.error('Failed to read data.json:'+str(e))
@bot.listener.on_startup
async def startup(room):
    global loop,servers
    loop = asyncio.get_running_loop()
    for server in servers:
        if server.room == room:
            if not hasattr(server,'market'): setattr(server,'market',None)
            loop.create_task(processpaper.check_depot(server),name='check-depot-'+server.name)
@bot.listener.on_message_event
async def bot_help(room, message):
    bot_help_message = f"""
    Help Message:
        prefix: {prefix}
        commands:
            create-depot:
                command: create-depot name [taxCostspercent] [tradingCosts] [tradingCostspercent] [currency]
                description: add depot
            add:
                command: add isin/ticker [count] [price] [depot]
                description: add an paper to an depot
            buy:
                command: buy isin/ticker [count][@limit] [price] [depot]
                description: buy an amount of paper
            sell:
                command: sell isin/ticker [count] [price] [depot]
                description: sell an amount of paper
            show:
                command: show [depot]
                description: show an overview of an depot
            analyze:
                command: analyze isin/ticker [strategy] [count] [date]
                description: analyze an paper
            overview:
                command: oveview style range [depot]
                description: show overview for all papers in depot
                style:
                    graphic: show basic text analys and plot of timerange
                    text: show basic text analys
                range: 30d per default, y,d,w allowed
            change-setting:
                command: change-setting depot/isin setting value
                    settings:
                        strategy: per depot/paper
                        datasource: per depot
                        market: per depot
            help:
                command: help, ?, h
                description: display help command
                """
    match = botlib.MessageMatch(room, message, bot, prefix)
    if match.is_not_from_this_bot() and (
       match.command("help") 
    or match.command("?") 
    or match.command("h")):
        await bot.api.send_text_message(room.room_id, bot_help_message)
def parse_human_readable_duration(duration_str):
    units = {'d': 'days', 'w': 'weeks', 'y': 'years'}
    value = int(duration_str[:-1])
    unit = duration_str[-1]

    if unit not in units:
        raise ValueError(f"Invalid time unit '{unit}', valid units are {', '.join(units.keys())}")

    kwargs = {units[unit]: value}
    return datetime.timedelta(**kwargs)
def calculate_roi(df):
    timeframes = [('1 hour', datetime.timedelta(hours=1)), 
                  ('1 day', datetime.timedelta(days=1)), 
                  ('1 month', datetime.timedelta(days=30)), 
                  ('1 year', datetime.timedelta(days=365)),
                  ('all', df.index.max() - df.index.min())]
    roi = {}
    for label, delta in timeframes:
        last_time = df.index.max()
        first_time = last_time - delta
        if first_time < df.index.min() or first_time > df.index.max():
            continue
        last_close_idx = df.index.searchsorted(last_time)
        first_close_idx = df.index.searchsorted(first_time)
        if last_close_idx >= len(df) or df.index[last_close_idx] > last_time:
            last_close_idx -= 1
        if first_close_idx >= len(df) or df.index[first_close_idx] > first_time:
            first_close_idx -= 1
        first_close = df.iloc[first_close_idx]['Close']
        last_close = df.iloc[last_close_idx]['Close']
        roi[label] = (last_close - first_close) / first_close * 100
    return roi
def rating_to_color(rating, min_rating=-2, max_rating=2):
    middle = min_rating+max_rating
    if rating > middle:
        if rating>max_rating: rating = max_rating
        r = 0
        g = 82+round(((255-82)/max_rating)*rating)
        b = 0
    else:
        if rating<min_rating: rating = min_rating
        r = 82+round(((255-82)/min_rating)*rating)
        g = 0
        b = 0
    return f"#{r:02x}{g:02x}{b:02x}"
def truncate_text(text, max_length):
    if len(text) <= max_length:
        return text
    truncated = text[:max_length].rstrip()
    last_space = truncated.rfind(' ')
    if last_space != -1:
        truncated = truncated[:last_space]
    return truncated+'...'
async def main():
    try:
        await bot.main()
    except BaseException as e:
        logging.error('bot main fails:'+str(e))
        os._exit(1)
processpaper.bot = bot
processpaper.servers = servers
processpaper.datasources = datasources
managepaper.bot = bot
managepaper.servers = servers
asyncio.run(main())