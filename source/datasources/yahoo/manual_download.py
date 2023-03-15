import pathlib,sys;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
import database,datetime,yfinance,sqlalchemy
if len(sys.argv)<=1:
    ticker = 'TSLA'
else:
    ticker = sys.argv[1]
query = database.session.query(database.Symbol)
rows = query.all()
for row in rows:
    ticker = row.ticker
    sym = database.session.query(database.Symbol).filter_by(ticker=ticker).first()
    if not sym: exit
    date_entry,first_date = database.session.query(database.MinuteBar,sqlalchemy.sql.expression.func.min(database.MinuteBar.date)).filter_by(symbol=sym).first()
    atck = yfinance.Ticker(ticker)
    while first_date:
        df = yfinance.download(ticker,start=first_date-datetime.timedelta(days=59),end=first_date,interval="1h",prepost=True)
        df.reset_index(inplace=True)
        r = sym.AppendData(df)
        if r == 0:
            break
        database.session.add(sym)
        database.session.commit()
        first_date = first_date-datetime.timedelta(days=59)
