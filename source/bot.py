from init import *
import pathlib,database,pandas_ta,importlib.util,logging
loop = None
lastsend = None
class Portfolio(Config):
    def __init__(self, room, **kwargs) -> None:
        super().__init__(room, **kwargs)
@bot.listener.on_message_event
async def tell(room, message):
    global servers,lastsend
    logging.info(str(message))
    if not message.body.startswith(prefix) and room.member_count==2:
        message.body = prefix+' '+message.body
    match = botlib.MessageMatch(room, message, bot, prefix)
    if (match.is_not_from_this_bot() and match.prefix())\
    and (match.command("buy")\
      or match.command("sell")\
      or match.command("add")):
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
                        price = database.GetActPrice(paper)
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
    and match.command("analyze"):
        depot = None
        for adepot in servers:
            if adepot.room == room.room_id and (adepot.name == depot or depot == None):
                depot = adepot
        if not depot is str:
            npaper = None
            found = False
            for paper in depot.papers:
                if paper['isin'] == match.args()[1]:
                    df = database.GetPaperData(paper,90)
                    pass
    elif (match.is_not_from_this_bot() and match.prefix())\
    and match.command("show"):
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
                    if 'name' in paper and paper['count'] > 0:
                        actprice = database.GetActPrice(paper)
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
paper_strategies = []
async def ProcessStrategy(paper,depot,data):
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
                paper_strategy = {
                        'isin': paper['isin'],
                        'strategy': st['mod'].Strategy(paper,depot,bot)
                    }
                paper_strategies.append(paper_strategy)
                break
    if paper_strategy:
        await paper_strategy['strategy'].next(data)
async def check_depot(depot,fast=False):
    global lastsend,servers
    while True:
        logging.info('checking depot '+str(depot.name))
        try:
            for datasource in datasources:
                if not fast:
                    uF = datasource['mod'].GetUpdateFrequency()
                    await asyncio.sleep(uF)
                await datasource['mod'].UpdateTickers(depot.papers)
            for paper in depot.papers:
                try:
                    df = database.GetPaperData(paper,30) 
                    await ProcessStrategy(paper,depot,df) 
                except BaseException as e:
                    if not hasattr(depot,'lasterror') or depot.lasterror != str(e):
                        await bot.api.send_text_message(depot.room,str(depot.name)+': '+str(e))
                        depot.lasterror = str(e)
            await save_servers()
            await asyncio.sleep(1)
        except BaseException as e:
            logging.error(str(e), exc_info=True)
            if not hasattr(depot,'lasterror') or depot.lasterror != str(e):
                await bot.api.send_text_message(depot.room,str(depot.name)+': '+str(e))
                depot.lasterror = str(e)
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
            stop:
                command: stop isin/ticker price [count]
                description: add an stop order for isin/ticker
            tsl:
                command: tsl isin/ticker price percent [count]
                description: add an tsl order for isin/ticker
            show:
                command: show [depot]
                description: show an overview of an depot
            analyze:
                command: analyze isin/ticker [strategy]
                description: analyze an paper
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