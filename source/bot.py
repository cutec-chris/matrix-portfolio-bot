from init import *
import pathlib,database,pandas_ta,importlib.util,logging,os,pandas,sqlalchemy.sql.expression,datetime,sys,backtrader,time,aiofiles,aiohttp,aiohttp.web,random,backtests,os
import managepaper,processpaper,os,traceback
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
loop = None
lastsend = None
class Portfolio(Config):
    def __init__(self, room, **kwargs) -> None:
        super().__init__(room, **kwargs)
@bot.listener.on_message_event
async def tell(room, message):
    try:
        global servers,lastsend
        logger.info(str(message))
        if not message.body.startswith(prefix) and room.member_count==2:
            message.body = prefix+' '+message.body
        match = botlib.MessageMatch(room, message, bot, prefix)
        if (match.is_not_from_this_bot() and match.prefix()):
            res = await bot.api.async_client.room_typing(room.room_id,True,timeout=30000)
        tuser = None
        if match.is_not_from_this_bot() and room.member_count==2:
            tuser = message.sender
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
            if tuser:
                pf.client = tuser
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
                    if tuser:
                        server.client = tuser
                await save_servers()
                await bot.api.send_text_message(room.room_id, 'ok')
        elif (match.is_not_from_this_bot() and match.prefix())\
        and match.command("restart"):
            pf = None
            for server in servers:
                if server.room == room.room_id:
                    pf = server
            if tuser:
                pf.client = tuser
                await save_servers()
            await bot.api.send_text_message(room.room_id, 'exitting...')
            os._exit(0)
    except BaseException as e:
        logger.error(str(e), exc_info=True)
        await bot.api.send_text_message(room,str(e))
    await bot.api.async_client.room_typing(room.room_id,False,0)
async def reactto(room,rereaction,msg_id):
    mcontent={
        "msgtype": "m.text",#or m.notice seems to be shown more transparent
        "body": f"Reaction: {rereaction}",
        }
    mcontent['m.relates_to'] = {
        "m.in_reply_to": {
            "event_id": msg_id,
            "rel_type":'m.annotation',
            "key": rereaction,
        }}
    await bot.api.async_client.room_send(room_id=room.room_id,
                                        message_type="m.room.message",
                                        content=mcontent)
@bot.listener.on_reaction_event
async def handle_reaction(room, event, reaction):
    msg_id = event.source['content']['m.relates_to']['event_id']
    events = await get_room_events(bot.api.async_client,room.room_id,50)
    original_event = None
    for event in events:
        if event.event_id == msg_id:
            original_event = event
            break
    if not original_event:
        return
    if reaction == '👍️':
        tell(room,original_event.body)
    else:
        await reactto(room,'👀',msg_id)
datasources = []
strategies = []
connection = None
try:
    logging.basicConfig(level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    logger.info('starting event loop...')
    loop = asyncio.new_event_loop()
    logger.info('loading config...')
    with open('data.json', 'r') as f:
        nservers = json.load(f)
        for server in nservers:
            if not 'papers' in server:
                server['papers'] = []
            servers.append(Portfolio(server))
    logger.info('loading (and starting) datasources...')
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
            logger.error(folder.name+':Failed to import datasource:'+str(e))
    logger.info('loading strategys...')
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
            logger.error('Failed to import strategy:'+str(e))
except BaseException as e:
    logger.error('Failed to read data.json:'+str(e))
news_task,dates_task = None,None
@bot.listener.on_startup
async def startup(room):
    global loop,servers,news_task,dates_task
    loop = asyncio.get_running_loop()
    await database.Init(loop)
    loop.create_task(restart_task())
    for server in servers:
        if server.room == room:
            if not news_task:
                news_task = loop.create_task(processpaper.check_news(server),name='update-news-'+server.name)
            if not dates_task:
                dates_task = loop.create_task(processpaper.check_dates(server),name='update-dates-'+server.name)
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
            create-order:
                command: order type isin/ticker params
                description: create an special order 
                    type: 
                        iplan (Investment plan) params: price day(in month) [depot]
                        limit (Limit order) params: limit count [depot]
                        tsl (Trailing Stop Loss) params: initial_stop trailing_perc [depot]
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
async def status_handler(request):
    try:
        global loop,servers,news_task,dates_task
        if news_task.done()\
        or dates_task.done():
            res = False
        else:
            res = True
    except:
        res = False
    if res:
        return aiohttp.web.Response(text="OK")
    else:
        os._exit(2)
        return aiohttp.web.HTTPServerError(text='Tasks not running')
async def main():
    try:
        def unhandled_exception(loop, context):
            msg = context.get("exception", context["message"])
            logger.error(f"Unhandled exception caught: {msg}")
            loop.default_exception_handler(context)
            os._exit(1)
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(unhandled_exception)
        app = aiohttp.web.Application()
        app.add_routes([aiohttp.web.get('/status', status_handler)])
        runner = aiohttp.web.AppRunner(app, access_log=None)
        await runner.setup()
        try:
            site = aiohttp.web.TCPSite(runner,port=9998)    
            await site.start()
        except BaseException as e:
            logging.warning('failed to start healthcheck: '+str(e))
        await bot.main()
        await bot.main()
    except BaseException as e:
        logger.error('bot main fails:'+str(e),stack_info=True)
        os._exit(1)
processpaper.bot = bot
processpaper.servers = servers
processpaper.datasources = datasources
processpaper.strategies = strategies
processpaper.save_servers = save_servers
managepaper.bot = bot
managepaper.servers = servers
managepaper.datasources = datasources
managepaper.strategies = strategies
managepaper.save_servers = save_servers
async def restart_task(pshutdown_time = None):
    if pshutdown_time:
        shutdown_time = pshutdown_time
    else:
        shutdown_time = datetime.datetime.now().replace(hour=7, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
    time_until_shutdown = (shutdown_time - datetime.datetime.now()).total_seconds()
    await asyncio.sleep(time_until_shutdown)
    logger.warning('planned shutting down')
    os._exit(1)
asyncio.run(main())