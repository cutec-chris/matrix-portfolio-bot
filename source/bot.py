from init import *
import pathlib,database,pandas_ta,importlib.util,logging,os,pandas,sqlalchemy.sql.expression,datetime,sys,backtrader,time,aiofiles,random
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
        await bot.api.async_client.room_typing(room,True, timeout=30000)
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
                            sym = database.session.query(database.Symbol).filter_by(isin=paper['isin'],marketplace=depot.market).first()
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
                    datafound = False 
                    for datasource in datasources:
                        res,_ = await datasource['mod'].UpdateTicker(paper)
                        if hasattr(depot,'datasource') and depot.datasource == datasource['name']:
                            if res: break
                    if res: datafound = True
                    if not datafound:
                        await bot.api.send_text_message(room.room_id, 'no data avalible for symbol in (any) datasource, aborting...')
                        return
                    depot.papers.append(paper)
                if 'lastcount' in paper and count == None:
                    count = paper['lastcount']
                if paper['count'] > 0:
                    paper['lastcount'] = paper['count']
                db_depot = database.session.query(database.Depot).filter_by(room=depot.room,name=depot.name).first()
                if not db_depot:
                    db_depot = database.Depot(room=room.room_id, name=depot.name, taxCost=0, taxCostPercent=depot.taxCostPercent, tradingCost=depot.tradingCost, tradingCostPercent=depot.tradingCostPercent, currency=depot.currency, cash=0)
                    database.session.add(db_depot)
                sym = database.session.query(database.Symbol).filter_by(isin=match.args()[1],marketplace=depot.market).first()
                db_position = database.session.query(database.Position).filter_by(isin=paper['isin'], depot_id=db_depot.id).first()
                if not db_position:
                    db_position = database.Position(depot_id=db_depot.id,
                                        isin=paper['isin'],
                                        shares=paper['count'],
                                        price=paper['price'],
                                        ticker='')
                database.session.add(db_position)
                database.session.commit()
                for paper in depot.papers:
                    if paper['isin'] == match.args()[1]:
                        oldprice = float(paper['price'])
                        if not count:
                            count = 0
                        if not price:
                            price = oldprice
                        newprice = price*count
                        if 'ticker' in paper:
                            db_position.ticker = paper['ticker']
                        if match.command("buy"):
                            paper['price'] = oldprice+newprice
                            paper['count'] = paper['count']+count
                            db_position.shares = paper['count']
                            db_position.price = paper['price']
                            database.session.add(db_position)
                            db_trade = database.Trade(position_id=db_position.id,shares=count, price=price,datetime=datetime.datetime.now())
                            database.session.add(db_trade)
                        elif match.command("sell"):
                            if newprice>oldprice:
                                newprice = oldprice
                            paper['price'] = oldprice-newprice
                            paper['count'] = paper['count']-count
                            db_position.shares = paper['count']
                            db_position.price = paper['price']
                            database.session.add(db_position)
                            db_trade = database.Trade(position_id=db_position.id,shares=-count, price=price,datetime=datetime.datetime.now())
                            database.session.add(db_trade)
                        await save_servers()
                        database.session.commit()
                        await bot.api.send_text_message(room.room_id, 'ok')
                        break
        elif (match.is_not_from_this_bot() and match.prefix())\
        and match.command("analyze",case_sensitive=False):
            depot = None
            strategy = 'sma'
            range = '30d'
            if len(match.args())>3: range = match.args()[3]
            date = None
            if len(match.args())>4: strategy = match.args()[4]
            range = parse_human_readable_duration(range)
            for adepot in servers:
                if adepot.room == room.room_id and (adepot.name == depot or depot == None):
                    depot = adepot
            if not depot is str:
                if hasattr(depot,'strategy'):
                    strategy = depot.strategy
                if len(match.args())>2: strategy = match.args()[2]
                npaper = None
                found = False
                sym = database.session.query(database.Symbol).filter_by(isin=match.args()[1],marketplace=depot.market).first()
                if sym:
                    if sym.currency and sym.currency != depot.currency:
                        df = sym.GetConvertedData(datetime.datetime.utcnow()-range,None,depot.currency)
                    else:
                        df = sym.GetData(datetime.datetime.utcnow()-range)
                    vola = 0.0
                    for index, row in df.iterrows():
                        avola = ((row['High']-row['Low'])/row['Close'])*100
                        if avola > vola: vola = avola
                    msg = 'Analyse of %s (%s,%s) with %s\n' % (sym.name,sym.isin,sym.ticker,strategy)\
                            +'Open: %.2f Close: %.2f\n' % (float(df.iloc[0]['Open']),float(df.iloc[-1]['Close']))\
                            +'Change: %.2f\n' % (float(df.iloc[-1]['Close'])-float(df.iloc[0]['Close']))\
                            +'Last updated: %s\n' % (str(sym.GetActDate()))\
                            +'Volatility: %.2f\n' % vola
                    if sym.GetTargetPrice():
                        ratings = sym.GetTargetPrice()
                        msg += "Target Price: %.2f from %d Analysts (%s)\nAverage: %.2f\n" % (ratings)
                    if sym.GetFairPrice():
                        msg += "Fair Price: %.2f from %d Analysts (%s)\n" % (sym.GetFairPrice())
                    roi = calculate_roi(df)
                    for timeframe, value in roi.items():
                        msg += f"ROI for {timeframe}: {value:.2f}%\n"
                    ast = None
                    for st in strategies:
                        if st['name'] == strategy:
                            ast = st
                            break
                    if ast:
                        cerebro = database.BotCerebro(stdstats=False)
                        cerebro.addstrategy(ast['mod'].Strategy)
                        cerebro.broker.setcash(1000)
                        cerebro.addobserver(
                            backtrader.observers.BuySell,
                            barplot=True,
                            bardist=0.001)  # buy / sell arrows
                        #cerebro.addobserver(backtrader.observers.DrawDown)
                        #cerebro.addobserver(backtrader.observers.DataTrades)
                        cerebro.addobserver(backtrader.observers.Broker)
                        cerebro.addobserver(backtrader.observers.Trades)
                        initial_capital = cerebro.broker.getvalue()
                        cerebro.addsizer(backtrader.sizers.PercentSizer, percents=100)
                        def run_cerebro():
                            cerebro.adddata(backtrader.feeds.PandasData(dataname=df))
                            cerebro.run()
                            cerebro.saveplots(file_path = '/tmp/plot.jpeg')
                        await asyncio.get_event_loop().run_in_executor(None, run_cerebro)
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
                        await bot.api.send_image_message(room.room_id,'/tmp/plot.jpeg')
                    else:
                        await bot.api.send_markdown_message(room.room_id, msg)
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
                            sym = database.session.query(database.Symbol).filter_by(isin=paper['isin'],marketplace=depot.market).first()
                            if sym:
                                actprice = sym.GetActPrice(depot.currency)
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
                    msg = ''
        elif (match.is_not_from_this_bot() and match.prefix())\
        and match.command("overview",case_sensitive=False):
            tdepot = None
            msg = ''
            range = '30d'
            style = 'graphic'
            if len(match.args())>1:
                range = match.args()[1]
            if len(match.args())>2:
                style = match.args()[2]
            if len(match.args())>3:
                tdepot = match.args()[3]
            for depot in servers:
                if depot.room == room.room_id and (depot.name == tdepot or tdepot == None):
                    try:
                        range = parse_human_readable_duration(range)+datetime.timedelta(days=3)
                    except BaseException as e:
                        range = parse_human_readable_duration('33d')
                    msg = '<table style="text-align: right">\n'
                    msg += '<tr><th>Paper/Name</th><th>Analys</th><th>Change</th><th>Visual</th></tr>\n'
                    count = 0
                    async def overview_process(paper, depot, database, range, style, bot):
                        if not 'ticker' in paper: paper['ticker'] = ''
                        if not 'name' in paper: paper['name'] = paper['ticker']
                        sym = database.session.query(database.Symbol).filter_by(isin=paper['isin'],marketplace=depot.market).first()
                        if sym:
                            df = sym.GetDataHourly(datetime.datetime.utcnow()-range)
                            image_uri = None
                            if style == 'graphic':
                                if not (df.empty):
                                    cerebro = database.BotCerebro(stdstats=False)
                                    cdata = backtrader.feeds.PandasData(dataname=df)
                                    #cdata.addfilter(backtrader.filters.HeikinAshi(cdata))
                                    cerebro.adddata(cdata)
                                    fpath = '/tmp/%s.jpeg' % paper['isin']
                                    try:
                                        cerebro.run(stdstats=False)
                                        cerebro.saveplots(style='line',file_path = fpath,width=32*4, height=16*4,dpi=50,volume=False,grid=False,valuetags=False,linevalues=False,legendind=False,subtxtsize=4,plotlinelabels=False)
                                        async with aiofiles.open(fpath, 'rb') as tmpf:
                                            resp, maybe_keys = await bot.api.async_client.upload(tmpf,content_type='image/jpeg')
                                        image_uri = resp.content_uri
                                    except BaseException as e:
                                        image_uri = None
                                        logging.warning('failed to upload img:'+str(e))
                            analys = ''
                            if sym.GetTargetPrice():
                                ratings = sym.GetTargetPrice()
                                analys_t = "Target Price: %.2f from %d<br>(%s)<br>Average: %.2f<br>" % ratings
                                analys += f'<font color="{rating_to_color(ratings[3])}">{analys_t}</font>'
                            else: ratings = (0,0,'',0)
                            if sym.GetFairPrice():
                                analys += "Fair Price: %.2f from %d<br>(%s)<br>" % (sym.GetFairPrice())
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
                            try: roi_x = weighted_roi_sum(roi)
                            except: roi_x = 0
                            troi = ''
                            for timeframe, value in roi.items():
                                troi += f"ROI for {timeframe}: {value:.2f}%\n<br>"
                            result = {
                                "roi": roi_x,  # Berechneter ROI
                                "rating": ratings[3],
                                "msg_part": '<tr><td>' + paper['isin'] + '<br>%.0fx' % paper['count'] + paper['name'] +'</td><td>' + analys + '</td><td align=right>' + troi + '</td><td><img src="' + str(image_uri) + '"></img></td></tr>\n'
                            }
                            return result
                    tasks = []
                    for paper in depot.papers:
                        task = asyncio.create_task(overview_process(paper, depot, database, range, style, bot))
                        tasks.append(task)
                        count += 1
                    results = await asyncio.gather(*tasks)
                    filtered_results = list(filter(None, results))  # Filtere `None` Werte aus der Liste
                    sorted_results = sorted(filtered_results, key=lambda x: (x['roi'], x['rating']), reverse=False)  # Nach ROI sortieren
                    for result in sorted_results:
                        msg += result['msg_part']                  
                    msg += '</table>\n'
                    await bot.api.send_markdown_message(room.room_id, msg)
                    msg = ''
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
    await bot.api.async_client.room_typing(room,False)
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
            await bot.api.async_client.set_presence('online','updating '+" ".join(check_status))
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
                                    ShouldSave = ShouldSave or await ProcessStrategy(paper,depot,df) 
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
            await bot.api.async_client.set_presence('online','updating '+" ".join(check_status))
            if ShouldSave: 
                await save_servers()
            #Wait minimal one cyclus for the datasource
            await asyncio.sleep(UpdateTime-(time.time()-started))
        datasourcetasks = [asyncio.create_task(checkdatasource(datasource)) for datasource in datasources]
        await asyncio.wait(datasourcetasks)
        await bot.api.async_client.set_presence('unavailable','')
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
    logging.info('loading datasources...')
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
                  ('all', df.index.max()-df.index.min())]
    roi = {}
    for label, delta in timeframes:
        last_time = df.index.max()
        first_time = last_time - delta
        if first_time < df.index.min():
            continue
        last_close_idx = df.index.get_loc(last_time, method='nearest')
        first_close_idx = df.index.get_loc(first_time, method='nearest')
        first_close = df.iloc[first_close_idx]['Close']
        last_close = df.iloc[last_close_idx]['Close']
        roi[label] = (last_close - first_close) / first_close * 100
    return roi
def rating_to_color(rating_value, min_value=-2, max_value=2):
    def lerp(a, b, t):
        return a + (b - a) * t
    normalized_value = (rating_value - min_value) / (max_value - min_value)
    best_color = (0, 255, 0)  # Grün
    mid_color = (0, 0, 0)  # Schwarz
    worst_color = (255, 0, 0)  # Rot
    if normalized_value < 0.5:
        start_color, end_color = best_color, mid_color
        normalized_value *= 2
    else:
        start_color, end_color = mid_color, worst_color
        normalized_value = (normalized_value - 0.5) * 2
    color = tuple(int(lerp(a, b, normalized_value)) for a, b in zip(start_color, end_color))
    color_code = '#{:02x}{:02x}{:02x}'.format(*color)
    return color_code

bot.run()