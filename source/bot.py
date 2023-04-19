from init import *
import pathlib,database,pandas_ta,importlib.util,logging,os,pandas,sqlalchemy.sql.expression,datetime,sys,backtrader,time,aiofiles,random,multiprocessing
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
        if (match.is_not_from_this_bot() and match.prefix()):res = await bot.api.async_client.room_typing(room.room_id,True,timeout=30000)
        if (match.is_not_from_this_bot() and match.prefix())\
        and (match.command("buy",case_sensitive=False)\
        or match.command("sell",case_sensitive=False)\
        or match.command("remove",case_sensitive=False)\
        or match.command("add",case_sensitive=False)):
            depot = None
            count = None
            if len(match.args())>2:
                count = float(match.args()[2])
            price = None
            if len(match.args())>4:
                depot = match.args()[4]
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
                            sym = connection.session.query(database.Symbol).filter_by(isin=paper['isin'],marketplace=depot.market).first()
                            if sym: 
                                price = sym.GetActPrice(connection.session)
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
                    res = False
                    for datasource in datasources:
                        if hasattr(datasource['mod'],'UpdateTicker'):
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
                db_depot = connection.session.query(database.Depot).filter_by(room=depot.room,name=depot.name).first()
                if not db_depot:
                    db_depot = database.Depot(room=room.room_id, name=depot.name, taxCost=0, taxCostPercent=depot.taxCostPercent, tradingCost=depot.tradingCost, tradingCostPercent=depot.tradingCostPercent, currency=depot.currency, cash=0)
                    connection.session.add(db_depot)
                sym = connection.session.query(database.Symbol).filter_by(isin=match.args()[1],marketplace=depot.market).first()
                db_position = connection.session.query(database.Position).filter_by(isin=paper['isin'], depot_id=db_depot.id).first()
                if not db_position:
                    db_position = database.Position(depot_id=db_depot.id,
                                        isin=paper['isin'],
                                        shares=paper['count'],
                                        price=paper['price'],
                                        ticker='')
                connection.session.add(db_position)
                connection.session.commit()
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
                            connection.session.add(db_position)
                            db_trade = database.Trade(position_id=db_position.id,shares=count, price=price,datetime=datetime.datetime.now())
                            connection.session.add(db_trade)
                        elif match.command("sell"):
                            if newprice>oldprice:
                                newprice = oldprice
                            paper['price'] = oldprice-newprice
                            paper['count'] = paper['count']-count
                            db_position.shares = paper['count']
                            db_position.price = paper['price']
                            connection.session.add(db_position)
                            db_trade = database.Trade(position_id=db_position.id,shares=-count, price=price,datetime=datetime.datetime.now())
                            connection.session.add(db_trade)
                        elif match.command("remove"):
                            depot.papers.remove(paper)
                        await save_servers()
                        connection.session.commit()
                        await bot.api.send_text_message(room.room_id, 'ok')
                        break
        elif (match.is_not_from_this_bot() and match.prefix())\
        and match.command("analyze",case_sensitive=False):
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
                sym = connection.session.query(database.Symbol).filter_by(isin=match.args()[1],marketplace=depot.market).first()
                if sym:
                    if sym.currency and sym.currency != depot.currency:
                        df = sym.GetConvertedData(connection.session,datetime.datetime.utcnow()-trange,None,depot.currency)
                    else:
                        df = sym.GetData(connection.session,datetime.datetime.utcnow()-trange)
                    vola = 0.0
                    for index, row in df.iterrows():
                        avola = ((row['High']-row['Low'])/row['Close'])*100
                        if avola > vola: vola = avola
                    msg = 'Analyse of %s (%s,%s) with %s\n' % (sym.name,sym.isin,sym.ticker,strategy)\
                            +'Open: %.2f Close: %.2f\n' % (float(df.iloc[0]['Open']),float(df.iloc[-1]['Close']))\
                            +'Change: %.2f\n' % (float(df.iloc[-1]['Close'])-float(df.iloc[0]['Close']))\
                            +'Last updated: %s\n' % (str(sym.GetActDate(connection.session)))\
                            +'Volatility: %.2f\n' % vola
                    if sym.GetTargetPrice(connection.session):
                        ratings = sym.GetTargetPrice(connection.session)
                        msg += "Target Price: %.2f from %d Analysts (%s)\nAverage: %.2f\n" % (ratings)
                    if sym.GetFairPrice(connection.session):
                        msg += "Fair Price: %.2f from %d Analysts (%s)\n" % (sym.GetFairPrice(connection.session))
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
                            sym = connection.session.query(database.Symbol).filter_by(isin=paper['isin'],marketplace=depot.market).first()
                            if sym:
                                actprice = sym.GetActPrice(connection.session,depot.currency)
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
                    async def overview_process(paper, depot, trange, style, bot):
                        if not 'ticker' in paper: paper['ticker'] = ''
                        if not 'name' in paper: paper['name'] = paper['ticker']
                        sym = connection.session.query(database.Symbol).filter_by(isin=paper['isin'],marketplace=depot.market).first()
                        if sym:
                            df = sym.GetDataHourly(connection.session,datetime.datetime.utcnow()-trange)
                            await asyncio.sleep(0.05)
                            aprice = sym.GetActPrice(connection.session,depot.currency)
                            analys = 'Price: %.2f<br>From: %s' % (aprice,str(sym.GetActDate(connection.session)))+'<br>'
                            chance_price=0
                            if sym.GetTargetPrice(connection.session):
                                ratings = sym.GetTargetPrice(connection.session)
                                analys_t = "Target Price: %.2f from %d<br>(%s)<br>Average: %.2f<br>" % ratings
                                analys += f'<font color="{rating_to_color(ratings[3])}">{analys_t}</font>'
                                chance_price=((ratings[0]-aprice)/aprice)
                                analys += "Chance: %.2f %% in 1y<br>" % round(chance_price*100,1)
                            else: ratings = (0,0,'',0)
                            if sym.GetFairPrice(connection.session):
                                analys += "Fair Price: %.2f from %d<br>(%s)<br>" % (sym.GetFairPrice(connection.session))
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
                            return result
                    async def graphics_process(result):
                        image_uri = None
                        df = result['data']
                        paper = result['paper']
                        try:
                            if style == 'graphic':
                                if not (df.empty):
                                    fpath = '/tmp/%s.jpeg' % paper['isin']
                                    async def process_cerebro(df,fpath):
                                        cerebro = database.BotCerebro(stdstats=False)
                                        cdata = backtrader.feeds.PandasData(dataname=df)
                                        cerebro.adddata(cdata)
                                        try:
                                            cerebro.run(stdstats=False)
                                            cerebro.saveplots(style='line',file_path = fpath,width=32*4, height=16*4,dpi=50,volume=False,grid=False,valuetags=False,linevalues=False,legendind=False,subtxtsize=4,plotlinelabels=False)
                                        except BaseException as e:
                                            return None
                                            logging.warning('failed to process:'+str(e))
                                        return fpath
                                    image_uri = await run_in_thread(process_cerebro(df,fpath))
                                    try:
                                        async with aiofiles.open(image_uri, 'rb') as tmpf:
                                            resp, maybe_keys = await bot.api.async_client.upload(tmpf,content_type='image/jpeg')
                                        image_uri = resp.content_uri
                                    except BaseException as e:
                                        image_uri = None
                                        logging.warning('failed to upload img:'+str(e))
                            result['msg_part'] = result['msg_part'].replace('<img src=""></img>','<img src="' + str(image_uri) + '"></img>')
                        except BaseException as e: logging.warning(str(e))
                        return result
                    tasks = []
                    for paper in depot.papers:
                        task = asyncio.create_task(overview_process(paper, depot, trange, style, bot))
                        tasks.append(task)
                        count += 1
                    results = await asyncio.gather(*tasks)
                    filtered_results = list(filter(None, results))  # Filtere `None` Werte aus der Liste
                    sorted_results = sorted(filtered_results, key=lambda x: x['sort'], reverse=False)  # Nach ROI sortieren
                    def chunks(lst, n):
                        for i in range(0, len(lst), n):
                            yield lst[i:i + n]
                    for chunk in chunks(sorted_results, 25):
                        chunk_tasks = []
                        for result in chunk:
                            ctask = asyncio.create_task(graphics_process(result))
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
    await bot.api.async_client.room_typing(room.room_id,False,0)
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
async def ProcessIndicator(paper,depot,data):
    res = False
    def heikin_ashi(data):
        ha_data = pandas.DataFrame(index=data.index)
        ha_data['HA_Open'] = (data['Open'] + data['Close']) / 2
        ha_data['HA_High'] = data[['High', 'Open', 'Close']].max(axis=1)
        ha_data['HA_Low'] = data[['Low', 'Open', 'Close']].min(axis=1)
        ha_data['HA_Close'] = (data['Open'] + data['High'] + data['Low'] + data['Close']) / 4
        return ha_data
    hdata = data.resample('4H').agg({
                            'Open':'first',
                            'High':'max',
                            'Low':'min',
                            'Close':'last'})
    data = heikin_ashi(hdata)
    if   data.iloc[-2]['HA_Close']>data.iloc[-2]['HA_Open']:# and data.iloc[-2]['HA_Low']==data.iloc[-2]['HA_Open']:
        act_indicator = True
    elif data.iloc[-2]['HA_Close']<data.iloc[-2]['HA_Open']:# and data.iloc[-2]['HA_High']==data.iloc[-2]['HA_Close']:
        act_indicator = False
    else:
        act_indicator = None
    if not act_indicator: trend_symbol = '⌄'
    else: trend_symbol = '⌃'
    msg1 = 'trend changes to %s for %s %s (%s)' % (trend_symbol,paper['isin'],paper['name'],paper['ticker'])
    if 'trend_up' in paper:
        if act_indicator != paper['trend_up']:
            if act_indicator==False and paper['count']>0: #Downward Trend on an paper we have
                await bot.api.send_text_message(depot.room,msg1)
            elif act_indicator==True: #Trend changes to upwards
                await bot.api.send_text_message(depot.room,msg1)
            res = True
    else: res = True
    paper['trend_up'] = act_indicator
    return res
async def ChangeDepotStatus(depot,newstatus):
    global servers
    if newstatus!='':
        await bot.api.async_client.set_presence('online',newstatus)
    else:
        await bot.api.async_client.set_presence('unavailable','')
    ntext = ''
    i=0
    for adepot in servers:
        if adepot.room == depot.room:
            sumactprice = 0
            sumprice = 0
            for paper in adepot.papers:
                if paper['count'] > 0:
                    sym = connection.session.query(database.Symbol).filter_by(isin=paper['isin'],marketplace=depot.market).first()
                    if sym:
                        actprice = sym.GetActPrice(connection.session,depot.currency)
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
async def check_depot(depot,fast=False):
    global lastsend,servers
    while True:
        updatedcurrencys = []
        check_status = []
        async def checkdatasource(datasource):
            started = time.time()
            ShouldSave = False
            FailedTasks = 0
            UpdateTime = datasource['mod'].GetUpdateFrequency()
            logging.info(depot.name+' starting updates for '+datasource['name'])
            check_status.append(datasource['name'])
            await ChangeDepotStatus(depot,'updating '+" ".join(check_status))
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
                                    '_updated': sym.GetActDate(connection.session)
                                }
                                await datasource['mod'].UpdateTicker(currencypaper)
                                updatedcurrencys.append(sym.currency)
                            #Process strategy
                            if 'ticker' in paper and sym:
                                if sym:
                                    if sym.currency and sym.currency != depot.currency:
                                        df = sym.GetConvertedData(connection.session,(TillUpdated or datetime.datetime.utcnow())-datetime.timedelta(days=30*3),TillUpdated,depot.currency)
                                    else:
                                        df = sym.GetData(connection.session,(TillUpdated or datetime.datetime.utcnow())-datetime.timedelta(days=30*3),TillUpdated)
                                    ps = await ProcessStrategy(paper,depot,df)
                                    ShouldSave = ShouldSave or ps
                                    #ps = await ProcessIndicator(paper,depot,df)
                                    #ShouldSave = ShouldSave or ps
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
            if check_status == []:#when we only await UpdateTime we can set Status
                await ChangeDepotStatus(depot,'')
            else:
                await ChangeDepotStatus(depot,'updating '+" ".join(check_status))
            if ShouldSave: 
                await save_servers()
            #Wait minimal one cyclus for the datasource
            await asyncio.sleep(UpdateTime-(time.time()-started))
        #datasourcetasks = [asyncio.create_task(checkdatasource(datasource)) for datasource in datasources]
        #await asyncio.wait(datasourcetasks)
        await asyncio.sleep(1)
datasources = []
strategies = []
connection = None
try:
    logging.basicConfig(level=logging.INFO)
    logging.info('loading config...')
    with open('data.json', 'r') as f:
        nservers = json.load(f)
        for server in nservers:
            if not 'papers' in server:
                server['papers'] = []
            servers.append(Portfolio(server))
    logging.info('loading db...')
    connection = database.Connection()
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
while True:
    try:
        bot.run()
    except BaseException as e:
        logging.error(str(e))