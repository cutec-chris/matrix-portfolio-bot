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
    and match.command("buy")\
     or match.command("sell")\
     or match.command("add"):
        depot = None
        count = 0
        if len(match.args())>2:
            count = float(match.args()[2])
        price = None #TODO:getActPrice
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
            if not found:
                paper ={
                    'isin': match.args()[1],
                    'count': 0,
                    'price': 0
                }
                depot.papers.append(paper)
            for paper in depot.papers:
                if paper['isin'] == match.args()[1]:
                    oldprice = float(paper['price'])
                    newprice = price*count
                    if match.command("buy"):
                        paper['price'] = oldprice+newprice
                        paper['count'] = paper['count']+count
                    elif match.command("sell"):
                        paper['price'] = oldprice-newprice
                        paper['count'] = paper['count']-count
                    await save_servers()
                    await bot.api.send_text_message(room.room_id, 'ok')
                    break
    elif match.is_not_from_this_bot() and match.prefix()\
    and match.command("show"):
        tdepot = None
        msg = ''
        if len(match.args())>1:
            tdepot = match.args()[1]
        for depot in servers:
            if depot.room == room.room_id and (depot.name == tdepot or tdepot == None):
                msg += '<h3>%s</h3>' % depot.name
                msg += '<table>\n'
                msg += '<tr><th>Paper</th><th>Name</th><th>Price</th><th>Change</th></tr>\n'
                for paper in depot.papers:
                    if 'name' in paper:
                        actprice = yahoo.GetActPrice(paper)*paper['count']
                        change = actprice-paper['price']
                        msg += '<tr><td>'+paper['isin']+'</td><td>'+paper['name']+'</td><td>'+str(actprice)+'</td><td>'+str(change)+'</td></tr>\n'
                msg += '</table>\n'
        await bot.api.send_markdown_message(room.room_id, msg)
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
        loop.create_task(check_depot(pf))
        await save_servers()
        await bot.api.send_text_message(room.room_id, 'ok')
async def UpdatePaper(paper):
    if not (Data / ('%s.pkl' % paper['isin'])).exists():
        ticker = yahoo.get_symbol_for_isin(paper['isin'])
        paper['ticker'] = ticker
        await save_servers()
    data = yahoo.UpdateCSV(paper)
    npaper = yahoo.UpdateSettings(paper)
    paper['name'] = npaper['name']
    return data
async def check_depot(depot):
    global lastsend,servers
    while True:
        try:
            for paper in depot.papers:
                data = await UpdatePaper(paper)
        except BaseException as e:
            if not hasattr(depot,'lasterror') or depot.lasterror != str(e):
                await bot.api.send_text_message(depot.room,str(depot.name)+': '+str(e))
                depot.lasterror = str(e)
        await asyncio.sleep(60)
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
                command: add isin/ticker [count] [price] [depot]
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