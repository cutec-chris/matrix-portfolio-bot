import sqlalchemy,pathlib,enum,datetime,pandas,asyncio,backtrader,logging,csv,os,io,re,sqlalchemy.orm,sqlalchemy.ext.asyncio,random,time
from init import *
Base = sqlalchemy.orm.declarative_base()
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
class Depot(Base):
    __tablename__ = 'depot'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    room = sqlalchemy.Column(sqlalchemy.String(200), nullable=False)
    name = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)
    cash = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    taxCost = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    taxCostPercent = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    tradingCost = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    tradingCostPercent = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    currency = sqlalchemy.Column(sqlalchemy.String(5), nullable=False)
class Position(Base):
    __tablename__ = 'position'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    depot_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('depot.id'), nullable=False)
    ticker = sqlalchemy.Column(sqlalchemy.String(50), nullable=False)
    isin = sqlalchemy.Column(sqlalchemy.String(50), nullable=False)
    shares = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    price = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    depot = sqlalchemy.orm.relationship("Depot", backref="positions")
class Trade(Base):
    __tablename__ = 'trade'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    position_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('position.id'), nullable=False)
    datetime = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
    shares = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    price = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    position = sqlalchemy.orm.relationship("Position", backref="trades")
    """
    def ImportTrades(self,session,filename,onlydetect=False):
        def parse_date(date_str: str) -> datetime:
            DATE_FORMATS = ['%d/%m/%Y %H:%M', '%d.%m.%Y', '%d/%m/%Y']
            for fmt in DATE_FORMATS:
                try:
                    return datetime.datetime.strptime(date_str, fmt)
                except ValueError:
                    pass
            raise ValueError(f"Cannot parse date: {date_str}")
        def parse_shares(text: str) -> float:
            match = re.search(r"STK\s+(\d+)", text)
            if match:
                return float(match.group(1))
            return None
        def detect_delimiter(csv_data: str) -> str:
            possible_delimiters = [',', ';', '\t']
            delimiter_counts = {delim: 0 for delim in possible_delimiters}
            for line in csv_data[:5]:
                for delim in possible_delimiters:
                    delimiter_counts[delim] += line.count(delim)
            return max(delimiter_counts, key=delimiter_counts.get)
        def find_ticker(text):
            # Ein einfaches Muster, um den Ticker zu finden (angenommen, der Ticker besteht aus Großbuchstaben und Zahlen)
            ticker_pattern = re.compile(r'\b[A-Z0-9]+\b')
            match = ticker_pattern.search(text)
            if match:
                return match.group()
            return None
        def find_isin(text):
            # Ein Muster, um die ISIN zu finden (angenommen, die ISIN besteht aus zwei Großbuchstaben gefolgt von 10 alphanumerischen Zeichen)
            isin_pattern = re.compile(r'\b[A-Z]{2}[A-Z0-9]{10}\b')
            match = isin_pattern.search(text)
            if match:
                return match.group()
            return None
        with open(filename,'r') as f:
            csv_data = f.readlines()
        delimiter = detect_delimiter(csv_data)
        # Find the starting and ending line of the CSV data
        reader = csv.reader(io.StringIO(''.join(csv_data).strip()), delimiter=delimiter)
        header_ok = False
        double_pos = ['start_datetime','end_datetime','start_price','end_price','position']
        single_pos = ['end_datetime','end_price','position']
        while not header_ok:
            headers = next(reader)
            # Mapping column indices to relevant fields
            column_mapping = {
                "end_datetime": None,
                "end_price": None,
                "position": None
            }
            # Identifying column indices for the relevant fields
            for idx, header in enumerate(headers):
                if header in ["Datum der Öffnung"]:
                    column_mapping["start_datetime"] = idx
                elif header in ["Datum der Schließung", "Datum"]:
                    column_mapping["end_datetime"] = idx
                elif header in ["Betrag (€)", "Betrag"]:
                    column_mapping["amount"] = idx
                elif header in ["Startpreis"]:
                    column_mapping["start_price"] = idx
                elif header in ["Endpreis", "Valuta"]:
                    column_mapping["end_price"] = idx
                elif header in ["Instrument", "Verwendungszweck"]:
                    column_mapping["position"] = idx
                elif header in ["Richtung"]:
                    column_mapping["trade_type"] = idx
                elif header in ["Tradenummer"]:
                    column_mapping["trade_number"] = idx
            double_pos_present = all(key in column_mapping and column_mapping[key] is not None for key in double_pos)
            single_pos_present = all(key in column_mapping and column_mapping[key] is not None for key in single_pos)
            if double_pos_present:
                header_ok=True
                single_pos_present=False
                break
            elif single_pos_present:
                header_ok=True
                double_pos_present=False
                break
        # Importing data
        for row in reader:
            
            if not onlydetect:
                if double_pos_present:
                    trade = Trade(
                        datetime=pdate,
                        shares=shares,
                        price=float(price_str),
                        position_id=get_position_id(position_text, session)  # Assuming you have a function to get position_id
                    )
                    session.add(trade)
                    session.commit()
                elif single_pos_present:
                    trade = Trade(
                        datetime=pdate,
                        shares=shares,
                        price=float(price_str),
                        position_id=get_position_id(position_text, session)  # Assuming you have a function to get position_id
                    )
                    session.add(trade)
                    session.commit()
    """
class Market(enum.Enum):
    crypto = 'crypto'
    stock = 'stock'
    etf = 'etf'
    fund = 'fund'
    forex = 'forex'
    futures = 'futures'
    index = 'index'
class Symbol(Base):
    __tablename__ = 'symbol'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    ticker = sqlalchemy.Column(sqlalchemy.String(50), nullable=False)
    isin = sqlalchemy.Column(sqlalchemy.String(50), nullable=False)
    marketplace = sqlalchemy.Column(sqlalchemy.String(200), nullable=True)
    name = sqlalchemy.Column(sqlalchemy.String(200), nullable=False)
    market = sqlalchemy.Column(sqlalchemy.Enum(Market), nullable=False)
    active = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
    tradingstart = sqlalchemy.Column(sqlalchemy.DateTime)
    tradingend = sqlalchemy.Column(sqlalchemy.DateTime)
    currency = sqlalchemy.Column(sqlalchemy.String(5), nullable=False)
    async def AppendData(self,session, df):
        res = 0
        for index, row in df.iterrows():
            date = row["Datetime"]
            # Check if data for the date already exists
            existing_data = await session.execute(sqlalchemy.select(MinuteBar).filter_by(symbol=self, date=date))
            if existing_data.scalar():
                continue
            # Add new data if it doesn't exist
            session.add(MinuteBar(date=date, open=row["Open"], high=row["High"], low=row["Low"], close=row["Close"], volume=row["Volume"], symbol=self))
            res += 1
        return res
    async def GetData(self,session, start_date=None, end_date=None, timeframe='15m'):
        global ConnStr
        if timeframe == '15m':
            aggregator_func = None
        elif timeframe == '1h':
            if 'sqlite' in ConnStr:
                aggregator_func = sqlalchemy.func.strftime('%Y-%m-%d %H:00:00', MinuteBar.date)
            else:
                aggregator_func = sqlalchemy.func.date_format(MinuteBar.date, '%Y-%m-%d %H:00:00')
        elif timeframe == '1d':
            if 'sqlite' in ConnStr:
                aggregator_func = sqlalchemy.func.strftime('%Y-%m-%d', MinuteBar.date)
            else:
                aggregator_func = sqlalchemy.func.date_format(MinuteBar.date, '%Y-%m-%d')
        else:
            raise ValueError(f'Unsupported timeframe: {timeframe}')
        query = sqlalchemy.select(MinuteBar)
        query = query.filter_by(symbol=self)
        if start_date:
            query = query.filter(MinuteBar.date >= start_date)
        if end_date:
            query = query.filter(MinuteBar.date <= end_date)
        query = query.order_by(MinuteBar.date)
        if timeframe != '15m':
            query = sqlalchemy.select(
                aggregator_func.label('date'),
                sqlalchemy.func.min(MinuteBar.low).label('low'),
                sqlalchemy.func.max(MinuteBar.high).label('high'),
                sqlalchemy.func.first_value(MinuteBar.open).over(order_by=MinuteBar.date).label('open'),
                sqlalchemy.func.last_value(MinuteBar.close).over(order_by=MinuteBar.date).label('close'),
                sqlalchemy.func.sum(MinuteBar.volume).label('volume')
            ).filter_by(symbol=self)
            if start_date:
                query = query.filter(MinuteBar.date >= start_date)
            if end_date:
                query = query.filter(MinuteBar.date <= end_date)
            query = query.group_by(aggregator_func).order_by(aggregator_func)
            try:
                result = await session.execute(query)
                rows = [(row.date, row.open, row.high, row.low, row.close, row.volume) for row in result.all()]
            except BaseException as e:
                print(str(e))
            df = pandas.DataFrame(rows, columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])
            df['Datetime'] = pandas.to_datetime(df['Datetime'])
        else:
            result = await session.scalars(query)
            rows = [(row.date, row.open, row.high, row.low, row.close, row.volume) for row in result.fetchall()]
            df = pandas.DataFrame(rows, columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])
        df.set_index("Datetime", inplace=True)
        return df
    async def GetConvertedData(self,session, start_date=None, end_date=None, TargetCurrency=None, timeframe='15m'):
        excs = (await session.scalars(sqlalchemy.select(Symbol).filter_by(ticker='%s%s=X' % (TargetCurrency, self.currency)))).first()
        data = await self.GetData(session,start_date=start_date, end_date=end_date, timeframe=timeframe)
        if excs:
            exc = await excs.GetData(session,start_date=start_date, end_date=end_date, timeframe=timeframe)
            if not exc.empty:
                for index, row in data.iterrows():
                    # Den nächsten verfügbaren Wechselkurs suchen
                    if not exc.loc[exc.index >= index].empty:
                        exc_next = exc.loc[exc.index >= index].iloc[0]
                    else:
                        exc_next = exc.iloc[0]
                    # Umgerechnete Preise für diesen Zeitstempel berechnen
                    row['Open'] = row['Open'] / exc_next['Close']
                    row['High'] = row['High'] / exc_next['Close']
                    row['Low'] = row['Low'] / exc_next['Close']
                    row['Close'] = row['Close'] / exc_next['Close']
        elif TargetCurrency == self.currency:
            data = await self.GetData(session, start_date=start_date, end_date=end_date, timeframe=timeframe)
        return data
    async def GetDataHourly(self,session, start_date=None, end_date=None, TargetCurrency=None):
        return await self.GetConvertedData(session,start_date,end_date, TargetCurrency, timeframe='1h')
    async def GetActPrice(self,session, TargetCurrency=None):
        last_minute_bar = (await session.execute(sqlalchemy.select(MinuteBar).filter_by(symbol=self).order_by(MinuteBar.date.desc()))).scalars().first()
        if TargetCurrency and (TargetCurrency != self.currency):
            excs = (await session.execute(sqlalchemy.select(Symbol).filter_by(ticker='%s%s=X' % (TargetCurrency,self.currency)))).scalars().first()
            if excs:
                last_minute_bar.close = last_minute_bar.close / (await excs.GetActPrice(session))
            elif self.currency and (TargetCurrency != self.currency):
                return 0
        if last_minute_bar:
            return last_minute_bar.close
        else:
            return 0
    async def GetActDate(self,session):
        last_minute_bar = (await session.execute(sqlalchemy.select(MinuteBar).filter_by(symbol=self).order_by(MinuteBar.date.desc()))).scalars().first()
        if last_minute_bar:
            return last_minute_bar.date
        else:
            return 0
    async def GetTargetPrice(self,session, start_date=None, end_date=None):
        if start_date is None:
            start_date = datetime.datetime.now() - datetime.timedelta(days=90)
        if end_date is None:
            end_date = datetime.datetime.now()
        total_price_target = 0
        count = 0
        analyst_ratings = await session.execute(sqlalchemy.select(AnalystRating).filter_by(symbol_isin=self.isin))
        analyst_ratings = analyst_ratings.scalars().all()
        rating_count = {}
        total_rating = 0
        rating_weight = {'strong buy': 2, 'buy': 1, 'hold': 0, 'sell': -1, 'strong sell': -2}
        seen_ratings = set()
        rc = 0
        for rating in analyst_ratings:
            if start_date <= rating.date <= end_date:
                rating_key = (rating.name, rating.rating)
                if rating_key not in seen_ratings:
                    seen_ratings.add(rating_key)
                    if rating.target_price:
                        rc = 1
                        if rating.ratingcount: rc = rating.ratingcount
                        total_price_target += rating.target_price*rc
                        count += rc
                        if rating.rating in rating_count:
                            rating_count[rating.rating] += rc
                        else:
                            rating_count[rating.rating] = rc
                    try:
                        total_rating += rating_weight[rating.rating]*rc
                    except KeyError:
                        pass
        if count == 0:
            return None
        average_target_price = total_price_target / count
        rating_count_str = ", ".join(f"{k}: {v}" for k, v in rating_count.items())
        average_rating = total_rating / count
        return average_target_price, count, rating_count_str, average_rating
    async def GetFairPrice(self,session, start_date=None, end_date=None):
        if start_date is None:
            start_date = datetime.datetime.now() - datetime.timedelta(days=90)
        if end_date is None:
            end_date = datetime.datetime.now()
        total_price_target = 0
        count = 0
        rating_count = {}
        analyst_ratings = await session.execute(sqlalchemy.select(AnalystRating).filter_by(symbol_isin=self.isin))
        analyst_ratings = analyst_ratings.scalars().all()
        for rating in analyst_ratings:
            if start_date <= rating.date <= end_date:
                if rating.fair_price:
                    total_price_target += rating.fair_price
                    count += 1
                    if rating.rating in rating_count:
                        rating_count[rating.rating] += 1
                    else:
                        rating_count[rating.rating] = 1
        if count == 0:
            return None
        average_target_price = total_price_target / count
        rating_count_str = ", ".join(f"{k}: {v}" for k, v in rating_count.items())
        return average_target_price, count, rating_count_str
class MinuteBar(Base):
    __tablename__ = 'minute_bar'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False, index=True)
    open = sqlalchemy.Column(sqlalchemy.Float)
    high = sqlalchemy.Column(sqlalchemy.Float)
    low = sqlalchemy.Column(sqlalchemy.Float)
    close = sqlalchemy.Column(sqlalchemy.Float, index=True)
    volume = sqlalchemy.Column(sqlalchemy.Float)
    symbol_id = sqlalchemy.Column(sqlalchemy.Integer,
                sqlalchemy.ForeignKey('symbol.id',
                            onupdate="CASCADE",
                            ondelete="CASCADE"),
                nullable=False)
    symbol = sqlalchemy.orm.relationship('Symbol', backref='minute_bars')
    sqlalchemy.UniqueConstraint(symbol_id, date)
class Rating(enum.Enum):
    stromg_sell = 'strong sell'
    sell = 'sell'
    buy = 'buy'
    hold = 'hold'
    strong_buy = 'strong buy'
class AnalystRating(Base):
    __tablename__ = 'analyst_rating'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
    name = sqlalchemy.Column(sqlalchemy.String(200), nullable=True)
    target_price = sqlalchemy.Column(sqlalchemy.Float)
    fair_price = sqlalchemy.Column(sqlalchemy.Float)
    rating = sqlalchemy.Column(sqlalchemy.String(200),nullable=True)
    ratingcount = sqlalchemy.Column(sqlalchemy.Integer,nullable=True)
    symbol_isin = sqlalchemy.Column(sqlalchemy.String(50),
                nullable=False)
    symbol = sqlalchemy.orm.relationship(
        'Symbol',
        backref='analyst_ratings',
        primaryjoin='Symbol.isin == AnalystRating.symbol_isin',
        foreign_keys=symbol_isin,
    )
class EarningsCalendar(Base):
    __tablename__ = 'earnings_calendar'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    release_date = sqlalchemy.Column(sqlalchemy.Date, nullable=False)
    name = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)
    estimated_eps = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    symbol_isin = sqlalchemy.Column(sqlalchemy.String(50),
                nullable=False)
    symbol = sqlalchemy.orm.relationship(
        'Symbol',
        backref='earnings_calendars',
        primaryjoin='Symbol.isin == EarningsCalendar.symbol_isin',
        foreign_keys=symbol_isin,
    )
class NewsEntry(Base):
    __tablename__ = 'news'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    release_date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False, index=True)
    headline = sqlalchemy.Column(sqlalchemy.String(200))
    content = sqlalchemy.Column(sqlalchemy.Text)
    category = sqlalchemy.Column(sqlalchemy.String(100))
    source_id = sqlalchemy.Column(sqlalchemy.String(100))
    symbol_isin = sqlalchemy.Column(sqlalchemy.String(50),
                nullable=False)
    symbol = sqlalchemy.orm.relationship(
        'Symbol',
        backref='news',
        primaryjoin='Symbol.isin == NewsEntry.symbol_isin',
        foreign_keys=symbol_isin,
    )
class BotCerebro(backtrader.Cerebro):
    def __init__(self):
        super().__init__()
    def saveplots(cerebro, numfigs=1, iplot=True, start=None, end=None,
                width=160*4, height=90*4, dpi=100, tight=True, use=None, file_path = '', **kwargs):
        try:
            from backtrader import plot
            if cerebro.p.oldsync:
                plotter = plot.Plot_OldSync(**kwargs)
            else:
                plotter = plot.Plot(**kwargs)
            import matplotlib
            matplotlib.use('AGG')
            figs = []
            for stratlist in cerebro.runstrats:
                for si, strat in enumerate(stratlist):
                    rfig = plotter.plot(strat, figid=si * 100,
                                        numfigs=numfigs, iplot=iplot,
                                        start=start, end=end, use=use)
                    figs.append(rfig)

            for fig in figs:
                for f in fig:
                    f.set_size_inches(width / dpi, height / dpi)
                    f.savefig(file_path, dpi=dpi, bbox_inches='tight')
            return figs
        except BaseException as e:
            logger.warning(str(e))
engine = None
async def Init(loop):
    global engine,ConnStr
    if engine: return
    if config['sqlserver'] and config['sqlserver']['connstr']:
        ConnStr = config['sqlserver']['connstr']
        connect_args={
            }
    else:
        Data = pathlib.Path('.') / 'data' / 'database.db'
        Data.parent.mkdir(parents=True,exist_ok=True)
        connect_args={
            'timeout': 5,
            'check_same_thread': False,
            'isolation_level': None,
            }
        ConnStr='sqlite+aiosqlite:///'+str(Data)
    engine=sqlalchemy.ext.asyncio.create_async_engine(ConnStr, connect_args=connect_args,pool_size=50, max_overflow=60,pool_recycle=3600) 
    async def init_models():
        async with engine.begin() as conn:
            if 'sqlite' in ConnStr:
                res = await conn.execute(sqlalchemy.text("PRAGMA journal_mode=WAL2"))
            await conn.run_sync(Base.metadata.create_all)
    await init_models()
def new_session():
    return sqlalchemy.orm.sessionmaker(bind=engine, class_=sqlalchemy.ext.asyncio.AsyncSession, autocommit=False, autoflush=False)()
#logger.getLogger('sqlalchemy.engine').setLevel(logger.INFO)
async def FindSymbol(session,paper,market=None,CreateifNotExists=False):
    if 'isin' in paper and paper['isin']:
        sym = (await session.execute(sqlalchemy.select(Symbol).filter_by(isin=paper['isin'],marketplace=market).limit(1))).scalars().first()
    elif 'ticker' in paper and paper['ticker']:
        sym = (await session.execute(sqlalchemy.select(Symbol).filter_by(ticker=paper['ticker'],marketplace=market))).scalars().first()
    else: sym = None
    if not sym and CreateifNotExists:
        async with new_session() as sessionn,sessionn.begin():
            sym = Symbol(isin=paper['isin'],ticker=paper['ticker'],name=paper['name'],market=Market['stock'],marketplace=market,active=True)
            sym.currency = 'EUR'
            sessionn.add(sym)
            await sessionn.commit()
    if 'isin' in paper and paper['isin']:
        sym = (await session.execute(sqlalchemy.select(Symbol).filter_by(isin=paper['isin'],marketplace=market).limit(1))).scalars().first()
    elif 'ticker' in paper and paper['ticker']:
        sym = (await session.execute(sqlalchemy.select(Symbol).filter_by(ticker=paper['ticker'],marketplace=market))).scalars().first()
    else: sym = None
    return sym
class UpdateCyclic:
    def __init__(self, papers, market, name, UpdateFunc, delay=0, Waittime=60/3) -> None:
        self.papers = papers
        self.market = market
        self.WaitTime = Waittime
        self.Delay = delay
        self.UpdateFunc = UpdateFunc
        self.internal_delay_mult = {}
    async def calc_delay_func(self,paper):
        async with new_session() as session:
            sym = await FindSymbol(session,paper,self.market)
            if sym:
                if sym.market == Market.etf:
                    return 4
                if paper['count']>0:
                    return 1
                ratings = await sym.GetTargetPrice(session)
                if ratings and ratings[3] < 0:
                    return 16
                if ratings and ratings[3] < 0.5:
                    return 4
                return 2
        return None
    async def get_delay_mult(self,paper,default_mult):
        if not paper['isin'] in self.internal_delay_mult:
            res = await self.calc_delay_func(paper)
            if res:
                self.internal_delay_mult[paper['isin']] = res
            else:
                self.internal_delay_mult[paper['isin']] = default_mult
        if self.internal_delay_mult[paper['isin']] > default_mult:
            return self.internal_delay_mult[paper['isin']]
        else:
            return default_mult
    async def run(self):
        internal_updated = {}
        internal_delay_mult = {}
        while True:
            shuffled_papers = list(self.papers)
            random.shuffle(shuffled_papers)
            for paper in shuffled_papers:
                started = time.time()
                if not paper['isin'] in internal_delay_mult:
                    internal_delay_mult[paper['isin']] = 1
                try:
                    epaper = paper
                    if paper and (not internal_updated.get(epaper['isin']) or internal_updated.get(epaper['isin'])+datetime.timedelta(seconds=self.Delay*await self.get_delay_mult(epaper,internal_delay_mult[paper['isin']])) < datetime.datetime.now()):
                        res,till = await self.UpdateFunc(epaper,self.market)
                        if res and till: 
                            internal_updated[paper['isin']] = till
                            if internal_delay_mult[paper['isin']] > 5:
                                internal_delay_mult[paper['isin']] = 4
                            elif internal_delay_mult[paper['isin']] > 2:
                                internal_delay_mult[paper['isin']] -= 1
                        else:
                            internal_updated[paper['isin']] = datetime.datetime.now()
                            internal_delay_mult[paper['isin']] += 1
                        if self.WaitTime-(time.time()-started) > 0:
                            await asyncio.sleep(self.WaitTime-(time.time()-started))
                except BaseException as e:
                    logger.error(str(e))
            await asyncio.sleep(10)
db_lock = asyncio.Lock()
async def UpdateTickerProto(paper,market,DownloadChunc,SearchPaper,Minutes15=30,Hours=365,Days=10*365):
    global db_lock
    resp = None
    res = False
    olddate = None
    if (not 'name' in paper) or paper['name'] == None:
        resp = await SearchPaper(paper['isin'])
        if resp:
            paper['ticker'] = resp['symbol']
            if 'longname' in resp:
                paper['name'] = resp['longname']
            elif 'shortname' in resp:
                paper['name'] = resp['shortname']
            paper['type'] = resp['type'].lower()
        else:
            logger.warning('paper '+paper['isin']+' not found !')
            return False,None
    #download from latest date to now
    async with new_session() as session:
        sym = await FindSymbol(session,paper,market,True)
        if 'ticker' in paper and paper['ticker']:
            startdate = datetime.datetime.utcnow()-datetime.timedelta(days=30)
        if sym:
            result = await session.execute(sqlalchemy.select(MinuteBar, sqlalchemy.func.max(MinuteBar.date)).where(MinuteBar.symbol == sym))
            date_entry, latest_date = result.fetchone()
            startdate = latest_date
            if not latest_date:
                startdate = datetime.datetime.utcnow()-datetime.timedelta(days=Minutes15)
            if (not (sym.tradingstart and sym.tradingend))\
            or ((sym.tradingstart.time() <= datetime.datetime.utcnow().time() <= sym.tradingend.time()) and (datetime.datetime.utcnow().weekday() < 5))\
            or (latest_date and latest_date < datetime.datetime.utcnow()-datetime.timedelta(days=2)):
                while startdate < datetime.datetime.utcnow():
                    res,olddate = await DownloadChunc(session,sym,startdate,startdate+datetime.timedelta(days=Minutes15),'15m',paper,market)
                    startdate += datetime.timedelta(days=Minutes15)
                async with db_lock:
                    await session.commit()
    #download last 5 years if not there
    try:
        async with new_session() as session:
            sym = await FindSymbol(session,paper,market,True)
            startdate = datetime.datetime.utcnow()-datetime.timedelta(days=Hours)
            if sym:
                result = await session.execute(sqlalchemy.select(MinuteBar, sqlalchemy.func.min(MinuteBar.date)).where(MinuteBar.symbol == sym))
                date_entry, earliest_date = result.fetchone()
                if earliest_date:
                    enddate = earliest_date
                    todate = earliest_date-datetime.timedelta(days=round(Hours/10))
                    if todate < datetime.datetime.now()-datetime.timedelta(days=Hours):
                        todate = datetime.datetime.now()-datetime.timedelta(days=Hours)
                    res2,olddate2 = await DownloadChunc(session,sym,todate,earliest_date,'1h',paper,market)
                    async with db_lock:
                        await session.commit()
    except BaseException as e:
        logger.error(str(e))
    return res,olddate