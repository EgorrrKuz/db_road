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
        Инициализация всех экземпляров класса
        """

        self.boards = Boards()
        self.engines = Engines()
        self.history = History()
        self.markets = Markets()
        self.securities = Securities()

    async def main(self):
        """
        Создаем 1 сессию для всех классов
        """

        async with aiohttp.ClientSession() as session:
            # Разделяем задачи на потоки
            engines_t = asyncio.create_task(self.engines.start(session))
            markets_t = asyncio.create_task(self.markets.start(session))
            boards_t = asyncio.create_task(self.boards.start(session))
            securities_t = asyncio.create_task(self.securities.start(session))
            history_t = asyncio.create_task(self.history.start(session))

            # Запуск задач
            await engines_t
            await markets_t
            await boards_t
            await securities_t
            await history_t


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    app = App()
    asyncio.run(app.main())
