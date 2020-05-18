import json
import logging
import aiohttp
from db_road.main import Main


class REST(Main):
    @staticmethod
    def coincidences(server_data, name, data):
        # Сравнение с серверными данными
        if len(server_data[name]) > 0 and data is not None:
            for _data in server_data[name]:
                if _data.get('id') is not None:
                    _data.pop('id')
                if data == _data:
                    return True
        else:
            return True

        return False

    async def post(self, session, server_data, name, data):
        """
        Проверка на совпадение и загрузка в БД

        :param session: Сессия
        :param server_data: Данные с сервера
        :param name: Название таблицы
        :param data: Загружаемые данные
        """

        url: str = self.dataset_server.get(name)  # URL куда загружаем данные

        # Если совпадение не найдено: POST на сервер
        if self.coincidences(server_data, name, data) is False:
            try:
                async with session.post(url=url, data=json.dumps(data)) as resp:
                    logging.debug("POST to {}, status {}".format(url, resp.status))
            except aiohttp.ClientConnectorError:
                logging.error("Cannot connect to host {}".format(url))

    async def get(self, session, name):
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

    async def plunk(self, session, url, name, sub_source, server_data, new_data):
        try:
            # Загрузка данных с биржи
            async with session.get(url) as resp:
                logging.debug("GET from {}, status {}".format(url, resp.status))

                source: dict = await resp.json()  # Данные с биржи

                # Перебор массива данных и преобразование
                for data in source.get(name).get("data"):
                    # Проверка на совпадение и загрузка в БД
                    await self.post(session, server_data, name, new_data(sub_source, source, data))
        except aiohttp.ClientConnectorError:
            logging.error("Cannot connect to host {}".format(url))
