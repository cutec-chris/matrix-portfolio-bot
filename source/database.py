import sqlalchemy,sqlalchemy.orm,pathlib,enum
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
def GetActPrice(paper):
    sym = session.execute(sqlalchemy.select(Symbol).where(Symbol.isin==paper['isin'])).fetchone()
    if sym:
        sym = sym[0]
    else:
        return None
    date_entry,latest_date = session.query(MinuteBar,sqlalchemy.sql.expression.func.max(MinuteBar.date)).filter_by(symbol=sym).first()
    return date_entry.close
Data = pathlib.Path('.') / 'data'
Data.mkdir(parents=True,exist_ok=True)
dbEngine=sqlalchemy.create_engine('sqlite:///'+str(Data / 'database.db')) 
conn = dbEngine.connect()
Base.metadata.create_all(dbEngine)
SessionClass = sqlalchemy.orm.sessionmaker(bind=dbEngine)
session = SessionClass()