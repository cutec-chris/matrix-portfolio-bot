import pandas
class Strategy():
    def __init__(self,paper,depot,bot) -> None:
        self.paper = paper
        self.depot = depot
        self.bot = bot
    async def next(self,data):
        pass
    async def buy(self):
        if not 'lastreco' in self.paper or self.paper['lastreco'] != 'buy':
            self.paper['lastreco'] = 'buy'
            msg = 'buy '+self.paper['isin']
            if 'lastcount' in self.paper:
                msg += ' '+str(self.paper['lastcount'])
            await self.bot.api.send_text_message(self.depot.room,msg)
    async def sell(self):
        if not 'lastreco' in self.paper or self.paper['lastreco'] != 'sell':
            self.paper['lastreco'] = 'sell'
            msg = 'sell '+self.paper['isin']
            if 'lastcount' in self.paper:
                msg += ' '+str(self.paper['lastcount'])
            await self.bot.api.send_text_message(self.depot.room,msg)
