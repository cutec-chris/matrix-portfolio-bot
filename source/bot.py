from init import *
import yfinance
loop = None
lastsend = None
class Paper(Config):
    def __init__(self, room, **kwargs) -> None:
        super().__init__(room, **kwargs)
@bot.listener.on_message_event
async def tell(room, message):
    global servers,lastsend
    match = botlib.MessageMatch(room, message, bot, prefix)
    if match.is_not_from_this_bot() and match.prefix()\
    and match.command("add"):
        paper = Paper({
            'room': room.room_id,
            'portfolio': match.args()[1],
            'ticker': match.args()[2],
            'currency': 'EUR'
        })
        servers.append(paper)
        loop.create_task(check_paper(paper))
        await save_servers()
        await bot.api.send_text_message(room.room_id, 'ok')
async def check_paper(server):
    global lastsend,servers
    while True:
        try:
        except BaseException as e:
            if not hasattr(server,'lasterror') or server.lasterror != str(e):
                await bot.api.send_text_message(server.room,str(server.server)+': '+str(e))
                server.lasterror = str(e)
        await asyncio.sleep(5)
try:
    with open('data.json', 'r') as f:
        nservers = json.load(f)
        for server in nservers:
            servers.append(Paper(server))
except BaseException as e: 
    logging.error('Failed to read config.yml:'+str(e))
@bot.listener.on_startup
async def startup(room):
    global loop,servers
    loop = asyncio.get_running_loop()
    for server in servers:
        if server.room == room:
            loop.create_task(check_server(server))
@bot.listener.on_message_event
async def bot_help(room, message):
    bot_help_message = f"""
    Help Message:
        prefix: {prefix}
        commands:
            add:
                command: add portfolio ticker curency
                description: add paper
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