import database,sqlalchemy,logging,asyncio,datetime,aiofiles,pandas,os
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
bot = None
servers = None
datasources = None
async def manage_paper(room,message,match):
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
        async with database.new_session() as session:
            for paper in depot.papers:
                try:
                    if paper['isin'] == match.args()[1]\
                    or paper['ticker'] == match.args()[1]:
                        found = True
                        if not price:
                            sym = await database.FindSymbol(session,paper)
                            if sym: 
                                price = await sym.GetActPrice(session)
                            else:
                                price = 0
                        if not count:
                            count = paper['count']
                        break
                except: pass
        if not found:
            paper ={
                'isin': match.args()[1],
                'count': 0,
                'price': 0
            }
            datafound = False 
            res = False
            async with database.new_session() as session:
                sym = await database.FindSymbol(session,paper)
                if sym: 
                    datafound = True
                    if not price:
                        price = await sym.GetActPrice(session)
            for datasource in datasources:
                if hasattr(datasource['mod'],'UpdateTicker'):
                    res,oldddate = await datasource['mod'].UpdateTicker(paper)
                    if res or oldddate: break
            if res: datafound = True
            if not datafound:
                sym = await database.FindSymbol(session,paper)
                if sym: datafound = True
            if not datafound:
                await bot.api.send_text_message(room.room_id, 'no data avalible for symbol in (any) datasource, aborting...')
                return
            depot.papers.append(paper)
        if 'lastcount' in paper and count == None:
            count = paper['lastcount']
        if paper['count'] > 0:
            paper['lastcount'] = paper['count']
        async with database.new_session() as session,session.begin():
            db_depot = (await session.scalars(sqlalchemy.select(database.Depot).filter_by(room=depot.room, name=depot.name))).first()
            if not db_depot:
                db_depot = database.Depot(
                    room=room.room_id,
                    name=depot.name,
                    taxCost=0,
                    taxCostPercent=depot.taxCostPercent,
                    tradingCost=depot.tradingCost,
                    tradingCostPercent=depot.tradingCostPercent,
                    currency=depot.currency,
                    cash=0,
                )
                session.add(db_depot)
                await session.commit()
        async with database.new_session() as session,session.begin():
            db_depot = (await session.scalars(sqlalchemy.select(database.Depot).filter_by(room=depot.room, name=depot.name))).first()
            sym = (await session.scalars(sqlalchemy.select(database.Symbol).filter_by(isin=match.args()[1], marketplace=depot.market))).first()
            db_position = (await session.scalars(sqlalchemy.select(database.Position).filter_by(isin=paper["isin"], depot_id=db_depot.id))).first()
            if not db_position:
                db_position = database.Position(
                    depot_id=db_depot.id,
                    isin=paper["isin"],
                    shares=paper["count"],
                    price=paper["price"],
                    ticker="",
                )
                session.add(db_position)
                await session.commit()
        async with database.new_session() as session,session.begin():
            db_depot = (await session.scalars(sqlalchemy.select(database.Depot).filter_by(room=depot.room, name=depot.name))).first()
            sym = (await session.scalars(sqlalchemy.select(database.Symbol).filter_by(isin=match.args()[1], marketplace=depot.market))).first()
            db_position = (await session.scalars(sqlalchemy.select(database.Position).filter_by(isin=paper["isin"], depot_id=db_depot.id))).first()
            for paper in depot.papers:
                if paper['isin'] == match.args()[1] or paper['ticker'] == match.args()[1]:
                    async with session.begin_nested():
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
                            session.add(db_position)
                            db_trade = database.Trade(position_id=db_position.id,shares=count, price=price,datetime=datetime.datetime.now(tz=datetime.timezone.utc))
                            session.add(db_trade)
                        elif match.command("sell"):
                            if newprice>oldprice:
                                newprice = oldprice
                            paper['price'] = oldprice-newprice
                            paper['count'] = paper['count']-count
                            db_position.shares = paper['count']
                            db_position.price = paper['price']
                            session.add(db_position)
                            db_trade = database.Trade(position_id=db_position.id,shares=-count, price=price,datetime=datetime.datetime.now(tz=datetime.timezone.utc))
                            session.add(db_trade)
                        elif match.command("remove"):
                            depot.papers.remove(paper)
                        await save_servers()
                        await session.commit()
                        await bot.api.send_text_message(room.room_id, 'ok')
                        break
async def show(room,message,match):
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
            async with database.new_session() as session:
                for paper in depot.papers:
                    if paper['count'] > 0:
                        if not 'ticker' in paper: paper['ticker'] = ''
                        if not 'name' in paper: paper['name'] = paper['ticker']
                        sym = await database.FindSymbol(session,paper,depot.market)
                        if sym:
                            actprice = await sym.GetActPrice(session,depot.currency)
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
