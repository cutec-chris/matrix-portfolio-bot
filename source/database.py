import sqlalchemy,pathlib,enum,datetime,pandas,asyncio,backtrader,logging,csv,io,re,threading
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
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
    marketplace = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    name = sqlalchemy.Column(sqlalchemy.String(200), nullable=False)
    market = sqlalchemy.Column(sqlalchemy.Enum(Market), nullable=False)
    active = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
    tradingstart = sqlalchemy.Column(sqlalchemy.DateTime)
    tradingend = sqlalchemy.Column(sqlalchemy.DateTime)
    currency = sqlalchemy.Column(sqlalchemy.String(5), nullable=False)
    def AppendData(self, session, df):
        res = 0
        for index, row in df.iterrows():
            date = row["Datetime"]
            # Check if data for the date already exists
            existing_data = session.query(MinuteBar).filter_by(symbol=self, date=date).first()
            if existing_data:
                continue
            # Add new data if it doesn't exist
            session.add(MinuteBar(date=date, open=row["Open"], high=row["High"], low=row["Low"], close=row["Close"], volume=row["Volume"], symbol=self))
            res += 1
        return res
    def GetData(self, session, start_date=None, end_date=None, timeframe='15m'):
        if timeframe == '15m':
            aggregator_func = None
        elif timeframe == '1h':
            aggregator_func = sqlalchemy.func.strftime('%Y-%m-%d %H:00:00', MinuteBar.date)
        elif timeframe == '1d':
            aggregator_func = sqlalchemy.func.strftime('%Y-%m-%d', MinuteBar.date)
        else:
            raise ValueError(f'Unsupported timeframe: {timeframe}')
        query = session.query(MinuteBar).filter_by(symbol=self)
        if start_date:
            query = query.filter(MinuteBar.date >= start_date)
        if end_date:
            query = query.filter(MinuteBar.date <= end_date)
        query = query.order_by(MinuteBar.date)
        try:
            if timeframe != '15m':
                query = session.query(
                    aggregator_func.label('Datetime'),
                    sqlalchemy.func.min(MinuteBar.low).label('Low'),
                    sqlalchemy.func.max(MinuteBar.high).label('High'),
                    sqlalchemy.func.first_value(MinuteBar.open).over(order_by=MinuteBar.date).label('Open'),
                    sqlalchemy.func.last_value(MinuteBar.close).over(order_by=MinuteBar.date).label('Close'),
                    sqlalchemy.func.sum(MinuteBar.volume).label('Volume')
                ).filter_by(symbol=self)
                if start_date:
                    query = query.filter(MinuteBar.date >= start_date)
                if end_date:
                    query = query.filter(MinuteBar.date <= end_date)
                query = query.group_by('Datetime').order_by('Datetime')
                df = pandas.read_sql(query.statement, query.session.bind)
                df['Datetime'] = pandas.to_datetime(df['Datetime'])
            else:
                df = pandas.DataFrame(
                    [(row.date, row.open, row.high, row.low, row.close, row.volume) for row in query.all()],
                    columns=["Datetime", "Open", "High", "Low", "Close", "Volume"]
                )
            df.set_index("Datetime", inplace=True)
        except BaseException as e:
            logging.error(str(e),stack_info=True)
            return df 
        return df
    def GetConvertedData(self,session, start_date=None, end_date=None, TargetCurrency=None, timeframe='15m'):
        excs = session.query(Symbol).filter_by(ticker='%s%s=X' % (TargetCurrency,self.currency)).first()
        data = self.GetData(session,start_date,end_date, timeframe)
        if excs:
            exc = excs.GetData(session, start_date, end_date,timeframe)
            if not exc.empty:
                for index, row in data.iterrows():
                    # Den nächsten verfügbaren Wechselkurs suchen
                    if not exc.loc[exc.index >= index].empty:
                        exc_next = exc.loc[exc.index >= index].iloc[0] 
                    else: exc_next = exc.iloc[0]
                    # Umgerechnete Preise für diesen Zeitstempel berechnen
                    row['Open'] = row['Open'] / exc_next['Close']
                    row['High'] = row['High'] / exc_next['Close']
                    row['Low'] = row['Low'] / exc_next['Close']
                    row['Close'] = row['Close'] / exc_next['Close']
                return data
        elif TargetCurrency == self.currency:
            data = self.GetData(session, start_date,end_date)
            return data
        return data
    def GetDataHourly(self,session, start_date=None, end_date=None, TargetCurrency=None):
        return self.GetConvertedData(session,start_date,end_date, TargetCurrency, timeframe='1h')
    def GetActPrice(self, session, TargetCurrency=None):
        last_minute_bar = session.query(MinuteBar).filter_by(symbol=self).order_by(MinuteBar.date.desc()).first()
        if TargetCurrency:
            excs = session.query(Symbol).filter_by(ticker='%s%s=X' % (TargetCurrency,self.currency)).first()
            if excs:
                last_minute_bar.close = last_minute_bar.close / excs.GetActPrice()
            elif self.currency and (TargetCurrency != self.currency):
                return 0
        if last_minute_bar:
            return last_minute_bar.close
        else:
            return 0
    def GetActDate(self, session):
        last_minute_bar = session.query(MinuteBar).filter_by(symbol=self).order_by(MinuteBar.date.desc()).first()
        if last_minute_bar:
            return last_minute_bar.date
        else:
            return 0
    def GetTargetPrice(self, start_date=None, end_date=None):
        if start_date is None:
            start_date = datetime.datetime.now() - datetime.timedelta(days=90)
        if end_date is None:
            end_date = datetime.datetime.now()
        total_price_target = 0
        count = 0
        rating_count = {}
        total_rating = 0
        rating_weight = {'strong buy': 2, 'buy': 1, 'hold': 0, 'sell': -1, 'strong sell': -2}
        seen_ratings = set()
        for rating in self.analyst_ratings:
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
    def GetFairPrice(self, start_date=None, end_date=None):
        if start_date is None:
            start_date = datetime.datetime.now() - datetime.timedelta(days=90)
        if end_date is None:
            end_date = datetime.datetime.now()

        total_price_target = 0
        count = 0
        rating_count = {}
        for rating in self.analyst_ratings:
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
    date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
    open = sqlalchemy.Column(sqlalchemy.Float)
    high = sqlalchemy.Column(sqlalchemy.Float)
    low = sqlalchemy.Column(sqlalchemy.Float)
    close = sqlalchemy.Column(sqlalchemy.Float)
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
                sqlalchemy.ForeignKey('symbol.isin',
                            onupdate="CASCADE",
                            ondelete="CASCADE"),
                nullable=False)
    symbol = sqlalchemy.orm.relationship('Symbol', backref='analyst_ratings')
class EarningsCalendar(Base):
    __tablename__ = 'earnings_calendar'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    release_date = sqlalchemy.Column(sqlalchemy.Date, nullable=False)
    name = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)
    estimated_eps = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    symbol_isin = sqlalchemy.Column(sqlalchemy.String(50),
                sqlalchemy.ForeignKey('symbol.isin',
                            onupdate="CASCADE",
                            ondelete="CASCADE"),
                nullable=False)
    symbol = sqlalchemy.orm.relationship('Symbol', backref='earnings_calendar_entries')
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
            logging.warning(str(e))
class Connection:
    def __init__(self, Data=pathlib.Path('.') / 'data' / 'database.db'):
        Data.parent.mkdir(parents=True,exist_ok=True)
        self.dbEngine=sqlalchemy.create_engine('sqlite:///'+str(Data), poolclass=sqlalchemy.pool.StaticPool) 
        session_factory = sqlalchemy.orm.sessionmaker(bind=self.dbEngine)
        self.Session = sqlalchemy.orm.scoped_session(session_factory)
        conn = self.dbEngine.connect()
        Base.metadata.create_all(self.dbEngine)
        self.session = self.Session()
    def FindSymbol(self,paper,market=None):
        if 'isin' in paper and paper['isin']:
            sym = self.session.query(Symbol).filter_by(isin=paper['isin'],marketplace=market).first()
        elif 'ticker' in paper and paper['ticker']:
            sym = self.session.query(Symbol).filter_by(ticker=paper['ticker'],marketplace=market).first()
        else: sym = None
        return sym