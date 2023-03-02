import sqlalchemy,sqlalchemy.orm,pathlib,enum,datetime,pandas,asyncio,backtrader
Base = sqlalchemy.orm.declarative_base()
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
class Market(enum.Enum):
    crypto = 'crypto'
    stock = 'stock'
    forex = 'forex'
    futures = 'futures'
class Symbol(Base):
    __tablename__ = 'symbol'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    ticker = sqlalchemy.Column(sqlalchemy.String(50), nullable=False)
    isin = sqlalchemy.Column(sqlalchemy.String(50), nullable=False)
    name = sqlalchemy.Column(sqlalchemy.String(200), nullable=False)
    market = sqlalchemy.Column(sqlalchemy.Enum(Market), nullable=False)
    active = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
    tradingstart = sqlalchemy.Column(sqlalchemy.DateTime)
    tradingend = sqlalchemy.Column(sqlalchemy.DateTime)
    currency = sqlalchemy.Column(sqlalchemy.String(5), nullable=False)

    def AppendData(self, df):
        for index, row in df.iterrows():
            date = row["Datetime"]
            # Check if data for the date already exists
            existing_data = session.query(MinuteBar).filter_by(symbol=self, date=date).first()
            if existing_data:
                continue
            # Add new data if it doesn't exist
            session.add(MinuteBar(date=date, open=row["Open"], high=row["High"], low=row["Low"], close=row["Close"], volume=row["Volume"], symbol=self))
    def GetData(self, start_date=None, end_date=None):
        query = session.query(MinuteBar).filter_by(symbol=self)
        if start_date:
            query = query.filter(MinuteBar.date >= start_date)
        if end_date:
            query = query.filter(MinuteBar.date <= end_date)
        query = query.order_by(MinuteBar.date)
        df = pandas.DataFrame(
            [(row.date, row.open, row.high, row.low, row.close, row.volume) for row in query.all()], 
            columns=["Datetime", "Open", "High", "Low", "Close", "Volume"]
        )
        df.set_index("Datetime", inplace=True)
        return df
    def GetConvertedData(self,start_date=None, end_date=None, TargetCurrency=None):
        excs = session.query(Symbol).filter_by(ticker='%s%s=X' % (TargetCurrency,self.currency)).first()
        if excs:
            data = self.GetData(start_date,end_date)
            exc = excs.GetData(datetime.datetime.utcnow()-datetime.timedelta(days=30))
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
        return None
    def GetActPrice(self,TargetCurrency=None):
        last_minute_bar = session.query(MinuteBar).filter_by(symbol=self).order_by(MinuteBar.date.desc()).first()
        if TargetCurrency:
            excs = session.query(Symbol).filter_by(ticker='%s%s=X' % (TargetCurrency,self.currency)).first()
            if excs:
                last_minute_bar.close = last_minute_bar.close / excs.GetActPrice()
            elif TargetCurrency != self.currency:
                return 0
        if last_minute_bar:
            return last_minute_bar.close
        else:
            return 0
    def GetActDate(self):
        last_minute_bar = session.query(MinuteBar).filter_by(symbol=self).order_by(MinuteBar.date.desc()).first()
        if last_minute_bar:
            return last_minute_bar.date
        else:
            return 0
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
    strong_buy = 'strong buy'
class AnalystRating(Base):
    __tablename__ = 'analyst_rating'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
    target_price = sqlalchemy.Column(sqlalchemy.Float)
    rating = sqlalchemy.Column(sqlalchemy.Enum(Rating))
    symbol_id = sqlalchemy.Column(sqlalchemy.Integer,
                sqlalchemy.ForeignKey('symbol.id',
                            onupdate="CASCADE",
                            ondelete="CASCADE"),
                nullable=False)
    symbol = sqlalchemy.orm.relationship('Symbol', backref='analyst_ratings')

class BotCerebro(backtrader.Cerebro):
    def __init__(self):
        super().__init__()
    def saveplots(cerebro, numfigs=1, iplot=True, start=None, end=None,
                width=16*4, height=9*4, dpi=300, tight=True, use=None, file_path = '', **kwargs):
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
                f.savefig(file_path, bbox_inches='tight')
        return figs
Data = pathlib.Path('.') / 'data'
Data.mkdir(parents=True,exist_ok=True)
dbEngine=sqlalchemy.create_engine('sqlite:///'+str(Data / 'database.db')) 
conn = dbEngine.connect()
Base.metadata.create_all(dbEngine)
SessionClass = sqlalchemy.orm.sessionmaker(bind=dbEngine)
session = SessionClass()