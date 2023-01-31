import sqlalchemy,sqlalchemy.orm,pathlib,enum,datetime,pandas
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

def GetActPrice(paper):
    sym = session.execute(sqlalchemy.select(Symbol).where(Symbol.isin==paper['isin'])).fetchone()
    if sym:
        sym = sym[0]
    else:
        return None
    date_entry,latest_date = session.query(MinuteBar,sqlalchemy.sql.expression.func.max(MinuteBar.date)).filter_by(symbol=sym).first()
    return date_entry.close
def GetPaperData(paper,days):
    current_time = datetime.datetime.utcnow()
    datestart = current_time - datetime.timedelta(days=days)
    sym = session.execute(sqlalchemy.select(Symbol).where(Symbol.isin==paper['isin'])).fetchone()
    if sym:
        sym = sym[0]
    else:
        return None
    rows = session.query(MinuteBar).where(sqlalchemy.and_(MinuteBar.symbol==sym,MinuteBar.date>datestart)).order_by(MinuteBar.date).all()
    df = pandas.DataFrame(columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])
    for row in rows:
        entry = pandas.DataFrame.from_dict({
            "Datetime": [row.date],
            "Open":  [row.open],
            "High":  [row.high],
            "Low":  [row.low],
            "Close":  [row.close],
            "Volume":  [row.volume],
        })
        df = pandas.concat([df, entry], ignore_index=True)
    return df
Data = pathlib.Path('.') / 'data'
Data.mkdir(parents=True,exist_ok=True)
dbEngine=sqlalchemy.create_engine('sqlite:///'+str(Data / 'database.db')) 
conn = dbEngine.connect()
Base.metadata.create_all(dbEngine)
SessionClass = sqlalchemy.orm.sessionmaker(bind=dbEngine)
session = SessionClass()