import sys,pathlib;sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))
import database,sqlalchemy,asyncio,json

async def fetch_earnings_calendar(papers, api_key):
    symbols = ','.join([paper["ticker"] for paper in papers])
    async with aiohttp.ClientSession() as session:
        url = f'https://financialmodelingprep.com/api/v3/earnings_calendar?symbol={symbols}&apikey={api_key}'
        async with session.get(url) as response:
            if response.status == 200:
                earnings_data = await response.json()
                return earnings_data
            else:
                print(f"Error fetching earnings calendar for symbols {symbols}")
                return None

async def store_earnings_data(papers):
    earnings_data = await fetch_earnings_calendar(papers, 'your_api_key_here')
    if earnings_data:
        for entry in earnings_data:
            symbol = entry.get('symbol', None)
            release_date = entry.get('date', None)
            estimated_eps = entry.get('eps', None)
            if symbol and release_date:
                # Find the paper with the matching symbol
                paper = next((paper for paper in papers if paper["ticker"] == symbol), None)
                if paper:
                    symbol_id = paper["isin"]
                    existing_entry = database.session.query(EarningsCalendar).filter_by(symbol_id=symbol_id, release_date=release_date).first()
                    if not existing_entry:
                        new_entry = database.EarningsCalendar(symbol_id=symbol_id, release_date=release_date, estimated_eps=estimated_eps)
                        database.session.add(new_entry)
                        database.session.commit()

async def main():
    # JSON-Papiere aus einer Datei laden
    with open(pathlib.Path(__file__).parent.parent.parent.parent / 'data' / 'data.json', 'r') as f:
        papers = json.load(f)
    
    # Die store_earnings_data-Funktion asynchron aufrufen
    await store_earnings_data(papers[0]['papers'])

if __name__ == '__main__':
    asyncio.run(main())
