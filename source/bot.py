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