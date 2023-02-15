import sqlalchemy,sqlalchemy.orm,pathlib,enum,datetime,pandas,asyncio
Base = sqlalchemy.orm.declarative_base()
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
        df = pd.DataFrame(
            [(row.date, row.open, row.high, row.low, row.close, row.volume) for row in query.all()], 
            columns=["Datetime", "Open", "High", "Low", "Close", "Volume"]
        )
        return df
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

Data = pathlib.Path('.') / 'data'
Data.mkdir(parents=True,exist_ok=True)
dbEngine=sqlalchemy.create_engine('sqlite:///'+str(Data / 'database.db')) 
conn = dbEngine.connect()
Base.metadata.create_all(dbEngine)
SessionClass = sqlalchemy.orm.sessionmaker(bind=dbEngine)
session = SessionClass()