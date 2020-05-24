import aiohttp
import logging
from db_road.rest import REST


class Engines(REST):
    def __init__(self):
        super().__init__()
        self.name: str = "engines"  # Название таблицы

    def conversion(self, source: dict, data: dict):
        # Объединение массивов (столбцов и данных) в словарь
        new_data: dict = dict(zip(source.get(self.name).get("columns"), data))
        new_data.pop("id")  # Удаление столбца id

        return new_data

    async def start(self, session: aiohttp):
        while True:
            server_data: dict = await self.get(session, self.name)     # Целевые данные с сервера для проверки
            url: str = self.get_engines()                                   # URL откуда парсим данные

            if server_data is not None:
                # Загрузка данных с биржи
                try:
                    async with session.get(url, ) as resp:
                        logging.debug("GET from {}, status {}".format(url, resp.status))

                        source: dict = await resp.json()  # Данные с биржи

                        # Перебор массива данных и преобразование
                        for data in source.get("engines").get("data"):
                            # Проверка на совпадение и загрузка в БД
                            await self.post(session, server_data, self.conversion(source, data), self.name)
                except aiohttp.ClientConnectorError:
                    logging.error("Cannot connect to host {}".format(url))
