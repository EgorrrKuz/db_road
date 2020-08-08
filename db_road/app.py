import asyncio
import aiohttp
import logging
from models.boards import Boards
from models.engines import Engines
from models.history import History
from models.markets import Markets
from models.securities import Securities


class App:
    def __init__(self):
        """
        Initializing all instances of a class
        """

        self.boards: Boards = Boards()
        self.engines: Engines = Engines()
        self.history: History = History()
        self.markets: Markets = Markets()
        self.securities: Securities = Securities()

    async def main(self):
        """
        Create 1 session for all classes
        """

        semaphore = asyncio.Semaphore(200)
        timeout = aiohttp.ClientTimeout(total=5)

        async with semaphore:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # Divide tasks into threads
                engines_t: asyncio = asyncio.create_task(self.engines.start(session))
                markets_t: asyncio = asyncio.create_task(self.markets.start(session))
                boards_t: asyncio = asyncio.create_task(self.boards.start(session))
                securities_t: asyncio = asyncio.create_task(self.securities.start(session))
                history_t: asyncio = asyncio.create_task(self.history.start(session))

                # Task launch
                await engines_t
                await markets_t
                await boards_t
                await securities_t
                await history_t


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    app: App = App()
    asyncio.run(app.main())
