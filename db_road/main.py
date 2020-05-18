import json
import aiohttp
import logging


class Main:
    def __init__(self):
        self.server_url: str = 'localhost'  # Для запуска на локальном сервере
        # server_url: str = 'rest'       # Для запуска в контейнере Docker
        self.server_port: str = '8080'  # Порт на котором работает REST API

        # Ссылки для парсинга данных с сервера
        self.dataset_server: dict = {
            'engines': 'http://' + self.server_url + ':' + self.server_port + '/engines',
            'markets': 'http://' + self.server_url + ':' + self.server_port + '/markets',
            'boards': 'http://' + self.server_url + ':' + self.server_port + '/boards',
            'securities': 'http://' + self.server_url + ':' + self.server_port + '/securities',
            'securities_moex': 'http://' + self.server_url + ':' + self.server_port + '/securities_moex'
        }

    # Методы для получения данных с биржи
    @staticmethod
    def get_engines():
        """
        Получить dataset engines
        :return: engines (JSON)
        """

        return 'http://iss.moex.com/iss/engines.json'

    @staticmethod
    def get_markets(engine):
        """
        Получить dataset markets по engine
        :param engine: Название engine (string)
        :return: markets (JSON)
        """

        return 'http://iss.moex.com/iss/engines/{}/markets.json'.format(engine)

    @staticmethod
    def get_boards(engine, market):
        """
        Получить dataset markets по engine-market
        :param engine: Название engine (string)
        :param market: Название market (string)
        :return: boards (JSON)
        """

        return 'http://iss.moex.com/iss/engines/{}/markets/{}/boards.json'.format(engine, market)

    @staticmethod
    def get_securities(engine, market, board):
        """
        Получить dataset securities по engine-market-board
        :param engine: Название engine (string)
        :param market: Название market (string)
        :param board: Название boards (string)
        :return: securities (JSON)
        """

        return 'http://iss.moex.com/iss/engines/{}/markets/{}/boards/{}/securities.json'.format(engine, market, board)

    @staticmethod
    def get_history(engine, market, board, date):
        """

        :param engine: Название engine (string)
        :param market: Название market (string)
        :param board: Название boards (string)
        :param date: Дата торгов (string: "%Y-%d-%m")
        :return: securities (JSON)
        """

        return 'http://iss.moex.com/iss/history/engines/{}/markets/{}/boards/{}/securities.json?date={}'.format(engine,
                                                                                                                market,
                                                                                                                board,
                                                                                                                date)

    @staticmethod
    def coincidences(server_data, name, data):
        coincidence: bool = False  # Совпадение

        # Сравнение с серверными данными
        if len(server_data[name]) > 0:
            for _data in server_data[name]:
                if _data.get('id') is not None:
                    _data.pop('id')
                if data == _data:
                    coincidence: bool = True
                    break

        return coincidence

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

    async def download(self, session, name):
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
