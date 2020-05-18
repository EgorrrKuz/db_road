import asyncio
import aiohttp
import logging
from db_road.models.boards import Boards
from db_road.models.engines import Engines
from db_road.models.history import History
from db_road.models.markets import Markets
from db_road.models.securities import Securities


class App:
    def __init__(self):
        """
        Инициализация всех экземпляров класса
        """

        self.boards: Boards = Boards()
        self.engines: Engines = Engines()
        self.history: History = History()
        self.markets: Markets = Markets()
        self.securities: Securities = Securities()

    async def main(self):
        """
        Создаем 1 сессию для всех классов
        """

        async with aiohttp.ClientSession() as session:
            # Разделяем задачи на потоки
            engines_t: asyncio = asyncio.create_task(self.engines.start(session))
            markets_t: asyncio = asyncio.create_task(self.markets.start(session))
            boards_t: asyncio = asyncio.create_task(self.boards.start(session))
            securities_t: asyncio = asyncio.create_task(self.securities.start(session))
            history_t: asyncio = asyncio.create_task(self.history.start(session))

            # Запуск задач
            await engines_t
            await markets_t
            await boards_t
            await securities_t
            await history_t


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    app: App = App()
    asyncio.run(app.main())
