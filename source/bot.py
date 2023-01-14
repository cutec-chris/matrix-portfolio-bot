from init import *
import yahoo,pathlib
loop = None
lastsend = None
class Portfolio(Config):
    def __init__(self, room, **kwargs) -> None:
        super().__init__(room, **kwargs)
Data = pathlib.Path('.') / 'data'
if not Data.exists():
    Data.mkdir(parents=True)
@bot.listener.on_message_event
async def tell(room, message):
    global servers,lastsend
    match = botlib.MessageMatch(room, message, bot, prefix)
    if match.is_not_from_this_bot() and match.prefix()\
    and match.command("add"):
        for depot in servers:
            if depot.room == room.room_id and depot.name == match.args()[1]:
                paper ={
                    'isin': match.args()[2],
                    'count': 0,
                    'price': 0
                }
                depot.papers.append(paper)
                await save_servers()
                await bot.api.send_text_message(room.room_id, 'ok')
    elif match.is_not_from_this_bot() and match.prefix()\
    and match.command("buy"):
        depot = None
        count = float(match.args()[2])
        price = None #TODO:getActPrice
        if len(match.args())>4:
            depot = float(match.args()[4])
        if len(match.args())>3:
            price = float(match.args()[3])
        for adepot in servers:
            if adepot.room == room.room_id and (adepot.name == depot or depot == None):
                for paper in adepot.papers:
                    if paper['isin'] == match.args()[1]:
                        oldprice = float(paper['price'])
                        newprice = price*count
                        paper['price'] = oldprice+newprice
                        paper['count'] = paper['count']+count
                        await save_servers()
                        await bot.api.send_text_message(room.room_id, 'ok')
    elif match.is_not_from_this_bot() and match.prefix()\
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
        loop.create_task(check_paper(pf))
        await save_servers()
        await bot.api.send_text_message(room.room_id, 'ok')
async def UpdatePaper(paper):
    if not (Data / ('%s.csv' % paper['isin'])).exists():
        ticker = yahoo.get_symbol_for_isin(paper['isin'])
        paper['ticker'] = ticker
        await save_servers()
    yahoo.UpdatePaper(paper['ticker'],Data / ('%s.csv' % paper['isin']))
async def check_depot(depot):
    global lastsend,servers
    while True:
        try:
            for paper in depot.papers:
                await UpdatePaper(paper)
        except BaseException as e:
            if not hasattr(depot,'lasterror') or depot.lasterror != str(e):
                await bot.api.send_text_message(depot.room,str(depot.name)+': '+str(e))
                depot.lasterror = str(e)
        await asyncio.sleep(5)
try:
    with open('data.json', 'r') as f:
        nservers = json.load(f)
        for server in nservers:
            servers.append(Portfolio(server))
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
                command: add depot isin [currency]
                description: add an paper to an depot
            buy:
                command: buy isin/ticker count [price] [depot]
                description: buy an amount of paper
            sell:
                command: sell isin/ticker count [price] [depot]
                description: sell an amount of paper
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
    if match.is_not_from_this_bot() and match.prefix() and (
       match.command("help") 
    or match.command("?") 
    or match.command("h")):
        await bot.api.send_text_message(room.room_id, bot_help_message)
bot.run()