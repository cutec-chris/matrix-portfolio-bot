import database,sqlalchemy,logging,backtrader,asyncio,datetime,random,backtests,aiofiles,pandas,PIL
logger = logging.getLogger(__name__)
bot = None
server = None
datasources = None
async def analyze(room,message,match):
    depot = None
    strategy = 'sma'
    trange = '30d'
    if len(match.args())>3: trange = match.args()[3]
    date = None
    if len(match.args())>4: strategy = match.args()[4]
    trange = parse_human_readable_duration(trange)
    for adepot in servers:
        if adepot.room == room.room_id and (adepot.name == depot or depot == None):
            depot = adepot
    if not depot is str:
        if hasattr(depot,'strategy'):
            strategy = depot.strategy
        if len(match.args())>2: strategy = match.args()[2]
        npaper = None
        found = False
        async with database.new_session() as session:
            sym = await database.FindSymbol(session,{'isin':match.args()[1]},market=depot.market)
            if sym:
                if sym.currency and sym.currency != depot.currency:
                    df = await sym.GetConvertedData(session,datetime.datetime.utcnow()-trange,None,depot.currency)
                else:
                    df = await sym.GetData(session,datetime.datetime.utcnow()-trange)
                vola = 0.0
                for index, row in df.iterrows():
                    avola = ((row['High']-row['Low'])/row['Close'])*100
                    if avola > vola: vola = avola
                ast = None
                for st in strategies:
                    if st['name'] == strategy\
                    or strategy.startswith(st['name']):
                        ast = st
                        strategy = st['name']
                        break
                msg = 'Analyse of %s (%s,%s) with %s\n' % (sym.name,sym.isin,sym.ticker,strategy)\
                        +'Open: %.2f Close: %.2f\n' % (float(df.iloc[0]['Open']),float(df.iloc[-1]['Close']))\
                        +'Change: %.2f\n' % (float(df.iloc[-1]['Close'])-float(df.iloc[0]['Close']))\
                        +'Last updated: %s\n' % (str(await sym.GetActDate(session)))\
                        +'Volatility: %.2f\n' % vola
                tarPrice = await sym.GetTargetPrice(session)
                if tarPrice:
                    ratings = tarPrice
                    msg += "Target Price: %.2f from %d Analysts (%s)\nAverage: %.2f\n" % (ratings)
                fairPrice = await sym.GetFairPrice(session)
                if fairPrice:
                    msg += "Fair Price: %.2f from %d Analysts (%s)\n" % (fairPrice)
                roi = calculate_roi(df)
                for timeframe, value in roi.items():
                    msg += f"ROI for {timeframe}: {value:.2f}%\n"
                if ast:
                    initial_capital=1000
                    try:
                        res,cerebro = await backtests.default_backtest(st['mod'].Strategy,data=df)
                    except BaseException as e:
                        logger.error(str(e))
                        cerebro = None
                if cerebro:
                    msg += 'Statistic ROI: %.2f\n' % (((cerebro.broker.getvalue() - initial_capital) / initial_capital)*100)
                    checkfrom = datetime.datetime.utcnow()-datetime.timedelta(days=30*3)
                    amsg = None
                    for order in cerebro._broker.orders:
                        if order.status == 4:
                            if order.executed.dt: orderdate = backtrader.num2date(order.executed.dt)
                            if orderdate > checkfrom:
                                size_sum = order.size
                                if order.isbuy():
                                    otyp = 'buy'
                                else:
                                    otyp = 'sell'
                                amsg = 'Last order from %s: %s %.2f' % (str(backtrader.num2date(order.executed.dt)),otyp,order.size)
                                if hasattr(order,'chance'):
                                    amsg += ' chance %.1f till %s' % (order.chance,oder.chancetarget)
                                amsg += '\n'
                    if amsg: msg += amsg
                    await bot.api.send_markdown_message(room.room_id, msg)
                    await plot_strategy(cerebro,depot)
                else:
                    await bot.api.send_markdown_message(room.room_id, msg)
            else:
                await bot.api.send_markdown_message(room.room_id, 'no symbol found')
async def overview(room,message,match):
    tdepot = None
    msg = ''
    trange = '30d'
    style = 'graphic'
    if len(match.args())>1:
        style = match.args()[1]
    if len(match.args())>2:
        trange = match.args()[2]
    if len(match.args())>3:
        tdepot = match.args()[3]
    for depot in servers:
        if depot.room == room.room_id and (depot.name == tdepot or tdepot == None):
            try:
                trange = parse_human_readable_duration(trange)+datetime.timedelta(days=3)
            except BaseException as e:
                trange = parse_human_readable_duration('33d')
            count = 0
            semaphore = asyncio.Semaphore(10)
            async def overview_process(paper, depot, trange, style, bot):
                async with semaphore:
                    async with database.new_session() as session:
                        if not 'ticker' in paper: paper['ticker'] = ''
                        if not 'name' in paper: paper['name'] = paper['ticker']
                        sym = await database.FindSymbol(session,paper,depot.market)
                        if sym:
                            df = await sym.GetDataHourly(session,datetime.datetime.utcnow()-trange)
                            await asyncio.sleep(0.05)
                            aprice = await sym.GetActPrice(session,depot.currency)
                            analys = 'Price: %.2f<br>From: %s' % (aprice,str(await sym.GetActDate(session)))+'<br>'
                            chance_price=0
                            ratings = await sym.GetTargetPrice(session)
                            if ratings:
                                ratings = await sym.GetTargetPrice(session)
                                analys_t = "Target Price: %.2f from %d<br>(%s)<br>Average: %.2f<br>" % ratings
                                analys += f'<font color="{rating_to_color(ratings[3])}">{analys_t}</font>'
                                chance_price=((ratings[0]-aprice)/aprice)
                                analys += "Chance: %.2f %% in 1y<br>" % round(chance_price*100,1)
                            else: ratings = (0,0,'',0)
                            if await sym.GetFairPrice(session):
                                analys += "Fair Price: %.2f from %d<br>(%s)<br>" % (await sym.GetFairPrice(session))
                            roi = calculate_roi(df)
                            def weighted_roi_sum(roi_dict):
                                weights = {
                                    "1 hour": 0.5,
                                    "1 day": 1,
                                    "1 month": 0.7,
                                    "1 year": 0.2,
                                    "all": 0.1,
                                }
                                weighted_sum = 0
                                for key, value in roi_dict.items():
                                    if key in weights:
                                        weighted_sum += value * weights[key]
                                return weighted_sum
                            try: roi_x = weighted_roi_sum(roi)/100
                            except: roi_x = 0
                            troi = ''
                            for timeframe, value in roi.items():
                                troi_t = f"ROI for {timeframe}: {value:.2f}%\n<br>"
                                troi += f'<font color="{rating_to_color(value,-10,10)}">{troi_t}</font>'
                            result = {
                                "paper": paper,
                                "roi": roi_x,  # Berechneter ROI
                                "chance": chance_price,
                                "sort": ((ratings[3]+chance_price+roi_x)/3),
                                "rating": ratings[3],
                                "data": df,
                                "msg_part": '<tr><td>' + paper['isin'] + '<br>%.0fx' % paper['count'] + truncate_text(paper['name'],30) +'</td><td>' + analys + '</td><td align=right>' + troi + '</td><td><img src=""></img></td></tr>\n'
                            }
                            await bot.api.async_client.room_typing(room.room_id,True,timeout=30000) #refresh typing
                            return result
            async def graphics_process(result):
                def crop_image(image_path, output_path,crop_left=30, crop_right=30):
                    with PIL.Image.open(image_path) as img:
                        width, height = img.size
                        new_width = width - crop_left - crop_right
                        img_cropped = img.crop((crop_left, 0, new_width, height))
                        img_cropped.save(output_path)
                image_uri = None
                df = result['data']
                paper = result['paper']
                try:
                    if style == 'graphic':
                        if not df.empty:
                            fpath = '/tmp/%s.jpeg' % paper['isin']
                            async def process_cerebro(df,fpath):
                                try:
                                    cerebro = database.BotCerebro(stdstats=False)
                                    cdata = backtrader.feeds.PandasData(dataname=df)
                                    cerebro.adddata(cdata)
                                    await backtests.run_backtest(cerebro)
                                    cerebro.saveplots(style='line',file_path = fpath,width=32*4, height=16*4,dpi=50,volume=False,grid=False,valuetags=False,linevalues=False,legendind=False,subtxtsize=4,plotlinelabels=False)
                                    crop_image(fpath,fpath,5,20)
                                except BaseException as e:
                                    logger.warning('failed to process:'+str(e))
                                    return None
                                return fpath
                            image_uri = await process_cerebro(df,fpath)
                            try:
                                async with aiofiles.open(image_uri, 'rb') as tmpf:
                                    resp, maybe_keys = await bot.api.async_client.upload(tmpf,content_type='image/jpeg')
                                image_uri = resp.content_uri
                            except BaseException as e:
                                image_uri = None
                                logger.warning('failed to upload img:'+str(e))
                    res = await bot.api.async_client.room_typing(room.room_id,True,timeout=30000) #refresh typing
                    result['msg_part'] = result['msg_part'].replace('<img src=""></img>','<img src="' + str(image_uri) + '"></img>')
                except BaseException as e: logger.warning(str(e))
                return result
            tasks = []
            results = []
            for paper in depot.papers:
                task = asyncio.create_task(overview_process(paper, depot, trange, style, bot),name='overview-'+paper['ticker'])
                tasks.append(task)
                count += 1
            results = await asyncio.gather(*tasks)
            filtered_results = list(filter(None, results))  # Filtere `None` Werte aus der Liste
            sorted_results = sorted(filtered_results, key=lambda x: x['sort'], reverse=False)  # Nach ROI sortieren
            def chunks(lst, n):
                for i in range(0, len(lst), n):
                    yield lst[i:i + n]
            for chunk in chunks(sorted_results, 10):
                chunk_tasks = []
                for result in chunk:
                    ctask = asyncio.create_task(graphics_process(result),name='overview-graphic-'+paper['ticker'])
                    chunk_tasks.append(ctask)
                chunk_results = await asyncio.gather(*chunk_tasks)
                msg = '<table style="text-align: right">\n'
                msg += '<tr><th>Paper/Name</th><th>Analys</th><th>Change</th><th>Visual</th></tr>\n'
                cidx = 0
                for result in chunk_results:
                    cidx += 1
                    if result:
                        msg += result['msg_part']
                    else:
                        msg += chunk[cidx]['msg_part']
                msg += '</table>\n'
                await bot.api.send_markdown_message(room.room_id, msg)
async def plot_strategy(cerebro,depot):
    cerebro.saveplots(style='line',file_path = '/tmp/plot.jpeg',volume=True,grid=True,valuetags=True,linevalues=False,legendind=False,subtxtsize=4,plotlinelabels=True)
    await bot.api.send_image_message(depot.room,'/tmp/plot.jpeg')
async def ProcessStrategy(paper,depot,data):
    cerebro = None
    res = False
    if not isinstance(data, pandas.DataFrame) or data.empty:
        return False
    strategy = 'sma'
    if 'strategy' in paper:
        strategy = paper['strategy']
    elif hasattr(depot,'strategy'):
        strategy = depot.strategy
    if hasattr(depot,'client'):
        tuser = depot.client
    else:
        tuser = 'user'
    for st in strategies:
        if st['name'] in strategy:
            logger.info(str(depot.name)+': processing ticker '+paper['ticker']+' till '+str(data.index[-1])+' with '+st['name'])
            try:
                res,cerebro = await backtests.default_backtest(st['mod'].Strategy,data=data,ticker=paper['ticker'],isin=paper['isin'])
            except BaseException as e:
                logger.error(str(e))
                cerebro = None
            if cerebro:
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
                        msg1 = '%s strategy %s propose buying %d x %s %s (%s) at %s' % (tuser,st['name'],round(size_sum),paper['isin'],paper['name'],paper['ticker'],orderdate)
                        if hasattr(order,'chance'):
                            msg1 += ' chance %.1f till %s' % (order.chance,oder.chancetarget)
                        msg1 += '\n'
                        msg2 = 'buy %s %d' % (paper['isin'],round(size_sum))
                        if paper['count']>0: return False
                    else:
                        msg1 = '%s strategy %s propose selling %d x %s %s (%s) at %s' % (tuser,st['name'],round(-size_sum),paper['isin'],paper['name'],paper['ticker'],orderdate)
                        msg2 = 'sell %s %d' % (paper['isin'],round(-size_sum))
                        if paper['count']==0: return False
                    if strategy+':'+msg2 != paper['lastreco']:
                        paper['lastreco'] = strategy+':'+msg2
                        paper['lastcheck'] = orderdate.strftime("%Y-%m-%d %H:%M:%S")
                        await plot_strategy(cerebro,depot)
                        await bot.api.send_text_message(depot.room,msg1)
                        await bot.api.send_text_message(depot.room,msg2)
                        res = True
    return res
async def ChangeDepotStatus(depot,newstatus):
    global servers
    try:
        if newstatus!='':
            await bot.api.async_client.set_presence('online',newstatus)
        else:
            await bot.api.async_client.set_presence('unavailable','')
        ntext = ''
        i=0
        async with database.new_session() as session:
            for adepot in servers:
                if adepot.room == depot.room:
                    sumactprice = 0
                    sumprice = 0
                    for paper in adepot.papers:
                        if paper['count'] > 0:
                            sym = await database.FindSymbol(session,paper)
                            if sym:
                                actprice = await sym.GetActPrice(session,depot.currency)
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
    except BaseException as e:
        logger.error('ChangeDepotStatus failed: '+str(e))
async def check_depot(depot,fast=False):
    global lastsend,servers,connection,news_task
    last_processed_minute_bar_id = 0
    async with database.new_session() as session:
        query = sqlalchemy.select(sqlalchemy.func.max(database.MinuteBar.id).label("max_id"))
        last_bar = await session.execute(query)
        for row in last_bar:
            if row.max_id:
                last_processed_minute_bar_id = row.max_id
    updates_running = False
    check_status = []
    next_minute = datetime.datetime.now()
    while True:
        logger.info(depot.name+' starting updates '+str(datetime.datetime.now()))
        check_status.append(depot.name)
        TillUpdated = None
        ShouldSave = False
        await ChangeDepotStatus(depot,'updating '+" ".join(check_status))
        next_minute = (next_minute + datetime.timedelta(minutes=1)).replace(second=0, microsecond=0)
        try:
            async with database.new_session() as session:
                query = sqlalchemy.select(database.MinuteBar.symbol_id, sqlalchemy.func.max(database.MinuteBar.id).label("max_id")).where(database.MinuteBar.id > last_processed_minute_bar_id).group_by(database.MinuteBar.symbol_id)
                new_bars = await session.execute(query)
                symbol_ids = []
                for row in new_bars:
                    symbol_ids.append(row[0])
                    if row.max_id>last_processed_minute_bar_id:
                        last_processed_minute_bar_id = row.max_id
                symbols_query = sqlalchemy.select(database.Symbol).where(database.Symbol.id.in_(symbol_ids))
                symbols = await session.execute(symbols_query)
                symbols = symbols.scalars().all()

                shuffled_papers = list(depot.papers)
                random.shuffle(shuffled_papers)
                targetmarket = None
                if hasattr(depot,'market'): targetmarket = depot.market
                for sym in symbols:
                    for paper in shuffled_papers:
                        if paper['isin'] == sym.isin and sym.marketplace == targetmarket:
                            #Process strategy
                            if sym.currency and sym.currency != depot.currency:
                                df = await sym.GetConvertedData(session,(TillUpdated or datetime.datetime.utcnow())-datetime.timedelta(days=30*3),TillUpdated,depot.currency)
                            else:
                                df = await sym.GetData(session,(TillUpdated or datetime.datetime.utcnow())-datetime.timedelta(days=30*3),TillUpdated)
                            await asyncio.sleep(0.1)
                            ps = await ProcessStrategy(paper,depot,df)
                            ShouldSave = ShouldSave or ps
                            await asyncio.sleep(0.1)
                            break
            logger.info(depot.name+' finished updates '+str(datetime.datetime.now()))
        except BaseException as e:
            logger.error(depot.name+' '+str(e))
        check_status.remove(depot.name)
        if check_status == []:#when we only await UpdateTime we can set Status
            await ChangeDepotStatus(depot,'')
        else:
            await ChangeDepotStatus(depot,'updating '+" ".join(check_status))
        if ShouldSave: 
            await save_servers()
        wait_time = (next_minute - datetime.datetime.now()).total_seconds()
        if wait_time<0: wait_time = 1
        await asyncio.sleep(wait_time)
        if not updates_running:
            loop = asyncio.get_running_loop()
            updates_running = True
            for datasource in datasources:
                mod_ = datasource['mod']
                if hasattr(mod_,'StartUpdate'):
                    loop.create_task(mod_.StartUpdate(depot.papers,depot.market,depot.name),name='update-ds-'+depot.name)
async def check_news(depot):
    global lastsend,servers,connection
    last_processed = 0
    async with database.new_session() as session:
        try:
            query = sqlalchemy.select(sqlalchemy.func.max(database.NewsEntry.id).label("max_id"))
            last_bar = await session.execute(query)
            for row in last_bar:
                last_processed = row.max_id
        except BaseException as e:
            logging.error(str(e))
    shown_ids = []
    while True:
        try:
            async with database.new_session() as session:
                query = sqlalchemy.select(database.NewsEntry).where(database.NewsEntry.id > last_processed)
                new_news = await session.scalars(query)
                for entry in new_news.all():
                    if entry.source_id in shown_ids:
                        continue
                    details = False
                    for tdepot in servers:
                        if tdepot.room == depot.room:
                            for tpaper in tdepot.papers:
                                if tpaper['isin'] == entry.symbol_isin:
                                    if tpaper['count']>0: #show only news for papers we own in detail
                                        details = True
                    msg = entry.symbol_isin+':'+entry.headline
                    if details:
                        msg+='<br>'+entry.content
                    await bot.api.send_markdown_message(depot.room, msg)
                    shown_ids.append(entry.source_id)
                    if entry.id > last_processed:
                        last_processed = entry.id
        except BaseException as e:
            logger.error('news show:'+str(e))
        await asyncio.sleep(60*2)
async def check_dates(depot):
    global lastsend,servers,connection
    while True:
        try:
            async with database.new_session() as session:
                today = datetime.datetime.now().date()
                tomorrow = today + datetime.timedelta(days=1)
                query = sqlalchemy.select(database.EarningsCalendar).where(
                    sqlalchemy.and_(
                        database.EarningsCalendar.release_date >= today,
                        database.EarningsCalendar.release_date <= tomorrow
                    )
                )                
                dates = await session.scalars(query)
                for entry in dates.all():
                    msg = entry.symbol_isin+': event '+entry.name+' on '+str(entry.release_date)
                    await bot.api.send_markdown_message(depot.room, msg)
        except BaseException as e:
            logger.error('check_dates:'+str(e))
        seconds_until_6_am = ((datetime.datetime.combine(datetime.datetime.today() + datetime.timedelta(days=1), datetime.time(6)) - datetime.datetime.now()).total_seconds())
        await asyncio.sleep(seconds_until_6_am)
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
        if 0 <= first_close_idx < len(df):
            first_close = df.iloc[first_close_idx]['Close']
        else:
            return roi
        if 0 <= last_close_idx < len(df):
            last_close = df.iloc[last_close_idx]['Close']
        else:
            return roi
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
