import os,asyncio,json,websockets,yaml,pathlib,logging
config = None
with open(pathlib.Path(__file__).parent / "config.yaml", "r") as file:
    config = yaml.safe_load(file)
StreamSessionID = None
async def connect_and_receive_minute_bars(tickers):
    async with websockets.connect(config['xtb']['base_url']) as websocket:
        # Authentifiziere und abonniere für Minute Bars
        logging.info('sucessfully connected')
        auth_data = {
            "command": "login",
            "arguments": {
                "userId": config['xtb']['user'],
                "password": config['xtb']['password'],
                "appName": "Portfolio-Managment"
            }
        }
        await websocket.send(json.dumps(auth_data))
        auth_response = await websocket.recv()
        auth_response_json = json.loads(auth_response)
        auth_success = False
        if auth_response_json.get("status") == True:
            StreamSessionID = auth_response_json.get("streamSessionId")
            auth_success = True
        if not auth_success:
            return
        logging.info('sucessfully authentificated')
        async with websockets.connect(config['xtb']['base_url']+'Stream') as websocket_s:
            logging.info('connected to streaming server')
            await websocket_s.send(json.dumps({
                "command": "getNews",
                "streamSessionId": StreamSessionID,
            }))
            for ticker in tickers:
                res = await websocket_s.send(json.dumps({
                    "command": "getCandles",
                    "streamSessionId": StreamSessionID,
                    "symbol": ticker,
                }))
                await asyncio.sleep(0.17)
            logging.info('%d symbols subscribed' % len(tickers))
            await websocket_s.send(json.dumps({
                "command": "getBalance",
                "streamSessionId": StreamSessionID,
            }))
            #await websocket_s.send(json.dumps({
            #    "command": "getKeepAlive",
            #    "streamSessionId": StreamSessionID,
            #}))
            while True:
                response = await websocket_s.recv()
                logging.info(f"< {response}")

async def StartUpdate(papers, market, name):
    if config:
        tickers = [paper["ticker"] for paper in papers]
        task = asyncio.create_task(connect_and_receive_minute_bars(tickers))
        await task

if __name__ == '__main__':
    logging.root.setLevel(logging.INFO)
    papers = [
        {
            "isin": None,
            "ticker": "BITCOIN",
        },
        {
            "isin": None,
            "ticker": "ETHERIUM",
        },
        {
            "isin": None,
            "ticker": "SOLANA",
        },
        {
            "isin": None,
            "ticker": "EURUSD",
        },
        {
            "isin": None,
            "ticker": "UNISWAP",
        }
    ]
    asyncio.run(StartUpdate(papers, "market", "name"))