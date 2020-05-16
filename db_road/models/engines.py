from db_road.models.main import Main
import aiohttp
import logging


class Engines(Main):
    def __init__(self):
        self.name = "engines"  # Название таблицы

    async def start(self, session):
        while True:
            server_data = await self.download(session, self.name)  # Целевые данные с сервера для проверки
            url = self.get_engines()

            if server_data is not None:
                # Загрузка данных с биржи
                try:
                    async with session.get(url) as resp:
                        logging.debug("GET from {}, status {}".format(url, resp.status))

                        engines = await resp.json()  # Данные с биржи

                        # Перебор массива данных и преобразование
                        for data in engines.get("engines").get("data"):
                            # Объединение массивов (столбцов и данных) в словарь
                            new_data = dict(zip(engines.get(self.name).get("columns"), data))
                            new_data.pop("id")  # Удаление столбца id

                            # Проверка на совпадение и загрузка в БД
                            await self.post(session, server_data, self.name, new_data)
                except aiohttp.ClientConnectorError:
                    logging.error("Cannot connect to host {}".format(url))
