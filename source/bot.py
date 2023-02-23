from init import *
import pathlib,database,pandas_ta,importlib.util,logging,os,pandas,sqlalchemy.sql.expression,datetime,sys,backtrader,time
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
        if (match.is_not_from_this_bot() and match.prefix())\
        and (match.command("buy",case_sensitive=False)\
        or match.command("sell",case_sensitive=False)\
        or match.command("add",case_sensitive=False)):
            depot = None
            count = None
            if len(match.args())>2:
                count = float(match.args()[2])
            price = None
            if len(match.args())>4:
                depot = float(match.args()[4])
            if len(match.args())>3:
                price = float(match.args()[3])
            for adepot in servers:
                if adepot.room == room.room_id and (adepot.name == depot or depot == None):
                    depot = adepot
            if not depot is str:
                npaper = None
                found = False
                for paper in depot.papers:
                    if paper['isin'] == match.args()[1]:
                        found = True
                        if not price:
                            sym = database.session.query(database.Symbol).filter_by(isin=paper['isin']).first()
                            if sym: 
                                price = sym.GetActPrice()
                            else:
                                price = 0
                        if not count:
                            count = paper['count']
                        break
                if not found:
                    paper ={
                        'isin': match.args()[1],
                        'count': 0,
                        'price': 0
                    }
                    depot.papers.append(paper)
                if 'lastcount' in paper and count == None:
                    count = paper['lastcount']
                if paper['count'] > 0:
                    paper['lastcount'] = paper['count']
                for paper in depot.papers:
                    if paper['isin'] == match.args()[1]:
                        oldprice = float(paper['price'])
                        if not count:
                            count = 0
                        if not price:
                            price = oldprice
                        newprice = price*count
                        if match.command("buy"):
                            paper['price'] = oldprice+newprice
                            paper['count'] = paper['count']+count
                        elif match.command("sell"):
                            if newprice>oldprice:
                                newprice = oldprice
                            paper['price'] = oldprice-newprice
                            paper['count'] = paper['count']-count
                        await save_servers()
                        await bot.api.send_text_message(room.room_id, 'ok')
                        loop = asyncio.get_running_loop()
                        loop.create_task(check_depot(depot,True))
                        break
        elif (match.is_not_from_this_bot() and match.prefix())\
        and match.command("analyze",case_sensitive=False):
            depot = None
            strategy = 'sma'
            if len(match.args())>2: strategy = match.args()[2]
            days = 30
            if len(match.args())>3: days = float(match.args()[3])
            date = None
            if len(match.args())>4: date = match.args()[4]
            for adepot in servers:
                if adepot.room == room.room_id and (adepot.name == depot or depot == None):
                    depot = adepot
            if not depot is str:
                npaper = None
                found = False
                sym = database.session.query(database.Symbol).filter_by(isin=match.args()[1]).first()
                if sym:
                    df = sym.GetData(datetime.datetime.utcnow()-datetime.timedelta(days=days))
                    vola = 0.0
                    for index, row in df.iterrows():
                        avola = ((row['High']-row['Low'])/row['Close'])*100
                        if avola > vola: vola = avola
                    msg = 'Analyse of %s (%s)\n' % (sym.name,sym.isin)\
                            +'Open: %.2f Close: %.2f\n' % (float(df.iloc[0]['Open']),float(df.iloc[-1]['Close']))\
                            +'Change: %.2f\n' % (float(df.iloc[-1]['Close'])-float(df.iloc[0]['Close']))\
                            +'Volatility: %.2f\n' % vola
                    await bot.api.send_markdown_message(room.room_id, msg)
                    for st in strategies:
                        if st['name'] == strategy:
                            cerebro = database.BotCerebro()
                            cerebro.addstrategy(st['mod'].Strategy)
                            cerebro.broker.setcash(1000)
                            cerebro.adddata(backtrader.feeds.PandasData(dataname=df))
                            cerebro.run()
                            cerebro.saveplots(file_path = '/tmp/plot.png')
                            await bot.api.send_image_message(room.room_id,'/tmp/plot.png')
                else:
                    await bot.api.send_markdown_message(room.room_id, 'no data for symbol found')
        elif (match.is_not_from_this_bot() and match.prefix())\
        and match.command("show",case_sensitive=False):
            tdepot = None
            msg = ''
            if len(match.args())>1:
                tdepot = match.args()[1]
            for depot in servers:
                if depot.room == room.room_id and (depot.name == tdepot or tdepot == None):
                    msg += '<h3>%s</h3>' % depot.name
                    msg += '<table style="text-align: right">\n'
                    msg += '<tr><th>Paper</th><th>Name</th><th>Price</th><th>Change</th></tr>\n'
                    sumprice = 0
                    sumactprice = 0
                    sellcosts = 0
                    for paper in depot.papers:
                        if paper['count'] > 0:
                            if not 'ticker' in paper: paper['ticker'] = ''
                            if not 'name' in paper: paper['name'] = paper['ticker']
                            sym = database.session.query(database.Symbol).filter_by(isin=paper['isin']).first()
                            if sym:
                                actprice = sym.GetActPrice()
                            else: 
                                actprice = 0
                            sumactprice += actprice*paper['count']
                            sumprice += paper['price']
                            change = (actprice*paper['count'])-paper['price']
                            sellcosts += ((sumactprice-sumprice)*(depot.tradingCostPercent/100))+depot.tradingCost
                            msg += '<tr><td>'+paper['isin']+'</td><td>%.0fx' % paper['count']+paper['name']+'</td><td align=right>%.2f (%.2f)' % ((actprice*paper['count']),actprice)+'</td><td align=right>%.2f' % change+'</td></tr>\n'
                    msg += '<tr><td></td><td></td><td align=right>%.2f' % sumactprice+'</td><td align=right>%.2f' % (sumactprice-sumprice)+'</td></tr>\n'
                    msg += '<tr><td></td><td></td><td>Taxes</td><td align=right>%.2f' % -((sumactprice-sumprice)*(depot.taxCostPercent/100))+'</td></tr>\n'
                    msg += '<tr><td></td><td></td><td>Sell-Costs</td><td align=right>%.2f' % -(sellcosts)+'</td></tr>\n'
                    msg += '<tr><td></td><td></td><td>Complete</td><td>%.2f' % ((sumactprice-sumprice)-(((sumactprice-sumprice)*(depot.taxCostPercent/100))+sellcosts))+'</td></tr>\n'
                    msg += '</table>\n'
            await bot.api.send_markdown_message(room.room_id, msg)
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
            loop.create_task(check_depot(pf))
            await save_servers()
            await bot.api.send_text_message(room.room_id, 'ok')
    except BaseException as e:
        logging.error(str(e), exc_info=True)
        if not hasattr(depot,'lasterror') or depot.lasterror != str(e):
            await bot.api.send_text_message(depot.room,str(depot.name)+': '+str(e))
            depot.lasterror = str(e)
paper_strategies = []
async def ProcessStrategy(paper,depot,data):
    cerebro = None
    paper_strategy = None
    for st in paper_strategies:
        if st['isin'] == paper['isin']:
            paper_strategy = st
    if paper_strategy == None:
        strategy = 'sma'
        if 'strategy' in paper:
            strategy = paper['strategy']
        for st in strategies:
            if st['name'] == strategy:
                cerebro = database.BotCerebro()
                cerebro.broker.setcash(1000)
                paper_strategy = {
                        'isin': paper['isin'],
                        'cerebro': cerebro
                    }
                cerebro.addstrategy(st['mod'].Strategy)
                paper_strategies.append(paper_strategy)
                break
    if paper_strategy and isinstance(data, pandas.DataFrame) and cerebro:
        try:
            def run_cerebro():
                paper_strategy['cerebro'].adddata(backtrader.feeds.PandasData(dataname=data))
                paper_strategy['cerebro'].run()
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
            if order.executed.dt: orderdate = backtrader.num2date(order.executed.dt)
            if orderdate > checkfrom:
                size_sum = order.size
                #print(order.isbuy(),order.size,orderdate)
        if size_sum != 0:
            if not 'lastreco' in paper: paper['lastreco'] = ''
            if size_sum > 0:
                msg1 = 'strategy %s propose buying %d x %s' % (strategy,round(size_sum),paper['isin'])
                msg2 = 'buy %s %d' % (paper['isin'],round(size_sum))
                if paper['count']>0: return False
            else:
                msg1 = 'strategy %s propose selling %d x %s' % (strategy,round(-size_sum),paper['isin'])
                msg2 = 'sell %s %d' % (paper['isin'],round(-size_sum))
                if paper['count']==0: return False
            if msg2 != paper['lastreco']:
                await bot.api.send_text_message(depot.room,msg1)
                await bot.api.send_text_message(depot.room,msg2)
                paper['lastreco'] = msg2
                paper['lastcheck'] = orderdate.strftime("%Y-%m-%d %H:%M:%S")
                return True
    return False
async def check_depot(depot,fast=False):
    global lastsend,servers
    while True:
        try:
            for paper in depot.papers:
                sym = database.session.query(database.Symbol).filter_by(isin=paper['isin']).first()
                date_entry,latest_date = database.session.query(database.MinuteBar,sqlalchemy.sql.expression.func.max(database.MinuteBar.date)).filter_by(symbol=sym).first()
                paper['_updated'] = latest_date
            for datasource in datasources:
                started = time.time()
                if not fast:
                    UpdateTime = datasource['mod'].GetUpdateFrequency()
                else:
                    UpdateTime = 0
                ShouldSave = False
                for paper in depot.papers:
                    await datasource['mod'].UpdateTicker(paper)
                    try:
                        sym = database.session.query(database.Symbol).filter_by(isin=paper['isin']).first()
                        if 'ticker' in paper and sym:
                            logging.info(str(depot.name)+': processing ticker '+paper['ticker'])
                            if sym:
                                df = sym.GetData(datetime.datetime.utcnow()-datetime.timedelta(days=30*3))
                                ShouldSave = ShouldSave or await ProcessStrategy(paper,depot,df) 
                    except BaseException as e:
                        logging.error(str(e), exc_info=True)
                if ShouldSave: 
                    await save_servers()
                await asyncio.sleep(UpdateTime-(time.time()-started))
            if fast:
                break
        except BaseException as e:
            logging.error(str(e), exc_info=True)
            if not hasattr(depot,'lasterror') or depot.lasterror != str(e):
                await bot.api.send_text_message(depot.room,str(depot.name)+': '+str(e))
                depot.lasterror = str(e)
            raise
datasources = []
strategies = []
try:
    logging.basicConfig(level=logging.INFO)
    logging.info('loading config...')
    with open('data.json', 'r') as f:
        nservers = json.load(f)
        for server in nservers:
            if not 'papers' in server:
                server['papers'] = []
            servers.append(Portfolio(server))
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
            logging.error('Failed to import datasource:'+str(e))
    for folder in (pathlib.Path(__file__).parent / 'strategy').glob('*'):
        try:
            spec = importlib.util.spec_from_file_location(folder.name, str(folder / '__init__.py'))
            mod_ = importlib.util.module_from_spec(spec)
            sys.modules[folder.name] = mod_
            spec.loader.exec_module(mod_)
            module = {
                    'name': folder.name,
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
            loop.create_task(check_depot(server))
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
            stop:
                command: stop isin/ticker price [count]
                description: add an stop order for isin/ticker
            tsl:
                command: tsl isin/ticker price percent [count]
                description: add an tsl order for isin/ticker
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
bot.run()