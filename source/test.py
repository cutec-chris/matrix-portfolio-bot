import yfinance, json

microsoft = yfinance.Ticker('MSF.DE')
print(microsoft.history(period='1d', interval='1m'))
daten = microsoft.info
print(f'{daten["regularMarketPrice"]} {daten["currency"]}')