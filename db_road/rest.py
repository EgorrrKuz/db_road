import json
import logging
import aiohttp
from db_road.main import Main


class REST(Main):
    @staticmethod
    def coincidences(server_data: dict, data: dict, name: str):
        # Сравнение с серверными данными
        if data is not None:
            for _data in server_data.get(name):
                if _data.get('id') is not None:
                    _data.pop('id')

                return data == _data
        else:
            return True

    async def post(self, session: aiohttp, server_data: dict, data: dict, name: str):
        """
        Проверка на совпадение и загрузка в БД

        :param session: Сессия
        :param server_data: Данные с сервера
        :param name: Название таблицы
        :param data: Загружаемые данные
        """

        url: str = self.dataset_server.get(name)  # URL куда загружаем данные

        # Если совпадение не найдено: POST на сервер
        if self.coincidences(server_data, data, name) is False:
            try:
                async with session.post(url=url, data=json.dumps(data)) as resp:
                    logging.debug("POST to {}, status {}".format(url, resp.status))
            except aiohttp.ClientConnectorError:
                logging.error("Cannot connect to host {}".format(url))

    async def get(self, session: aiohttp, name: str):
        """
        Загрузка данные с сервера

        :param session: Сессия
        :param name: Название таблицы
        :return: Данные с сервера (JSON)
        """

        url: str = self.dataset_server.get(name)  # URL откуда парсим данные

        # GET с сервера
        try:
            async with session.get(url) as resp:
                logging.debug("GET from {}, status {}".format(url, resp.status))

                return await resp.json()
        except aiohttp.ClientConnectorError:
            logging.error("Cannot connect to host {}".format(url))

    async def plunk(self, session: aiohttp, url: str, name: str, sub_source: dict, server_data: dict,
                    new_data: classmethod):
        try:
            # Загрузка данных с биржи
            async with session.get(url) as resp:
                logging.debug("GET from {}, status {}".format(url, resp.status))

                source: dict = await resp.json()  # Данные с биржи

                # Перебор массива данных и преобразование
                for data in source.get(name).get("data"):
                    # Проверка на совпадение и загрузка в БД
                    await self.post(session, server_data, new_data(sub_source, source, data),  name)
        except aiohttp.ClientConnectorError:
            logging.error("Cannot connect to host {}".format(url))
