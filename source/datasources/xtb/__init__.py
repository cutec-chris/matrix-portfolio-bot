import os,asyncio,json,websockets,yaml,pathlib,logging

with open(pathlib.Path(__file__).parent / "config.yaml", "r") as file:
    config = yaml.safe_load(file)
API_KEY = config["xtb"]["api_key"]
SECRET_KEY = config["xtb"]["api_secret"]

async def connect_and_receive_minute_bars(tickers,url):
    async with websockets.connect(url) as websocket:
        # Authentifiziere und abonniere für Minute Bars
        connect_response = await websocket.recv()
        logging.info('sucessfully connected')
        auth_data = {
            "action": "auth",
            "key": API_KEY,
            "secret": SECRET_KEY
        }
        await websocket.send(json.dumps(auth_data))
        auth_response = await websocket.recv()
        auth_response_json = json.loads(auth_response)
        auth_success = False
        for response_item in auth_response_json:
            if response_item.get("T") == "success" and response_item.get("msg") == "authenticated":
                auth_success = True
                break
        if not auth_success:
            return
        logging.info('sucessfully authentificated')
        subscription_data = {
            "action": "subscribe",
            "bars": tickers
        }
        await websocket.send(json.dumps(subscription_data))
        while True:
            response = await websocket.recv()
            logging.info(f"< {response}")

async def StartUpdate(papers, market, name):
    tickers = [paper["ticker"] for paper in papers]
    task = asyncio.create_task(connect_and_receive_minute_bars(tickers,CRYPTO_URL))
    await task

if __name__ == '__main__':
    logging.root.setLevel(logging.INFO)
    papers = [
        {
            "isin": None,
            "ticker": "BTC/USD",
        },
        {
            "isin": None,
            "ticker": "ETH/USD",
        },
        {
            "isin": None,
            "ticker": "SOL/USD",
        },
        {
            "isin": None,
            "ticker": "EUR/USD",
        },
        {
            "isin": None,
            "ticker": "UNI/USD",
        }
    ]
    asyncio.run(StartUpdate(papers, "market", "name"))