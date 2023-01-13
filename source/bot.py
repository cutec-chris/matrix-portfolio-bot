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
                    'count': 0
                }
                depot.papers.append(paper)
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
                'tradingCost': 0.0,
                'tradingCostPercent': 0.0,
                'currency': 'EUR',
                'papers': []
            })
        if len(match.args())>4:
            pf.currency = match.args()[4]
        if len(match.args())>3:
            pf.tradingCostPercent = float(match.args()[3])
        if len(match.args())>2:
            pf.tradingCost = float(match.args()[2])
        servers.append(pf)
        loop.create_task(check_paper(pf))
        await save_servers()
        await bot.api.send_text_message(room.room_id, 'ok')
async def check_depot(depot):
    global lastsend,servers
    while True:
        try:
            for paper in depot.papers:
                pass
        except BaseException as e:
            if not hasattr(server,'lasterror') or server.lasterror != str(e):
                await bot.api.send_text_message(server.room,str(server.server)+': '+str(e))
                server.lasterror = str(e)
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
            add:
                command: create-depot name [tradingCosts] [tradingCostspercent] [currency]
                description: add depot
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