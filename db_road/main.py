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
    def get_markets(engine: str):
        """
        Получить dataset markets по engine
        :param engine: Название engine (string)
        :return: markets (JSON)
        """

        return 'http://iss.moex.com/iss/engines/{}/markets.json'.format(engine)

    @staticmethod
    def get_boards(engine: str, market: str):
        """
        Получить dataset markets по engine-market
        :param engine: Название engine (string)
        :param market: Название market (string)
        :return: boards (JSON)
        """

        return 'http://iss.moex.com/iss/engines/{}/markets/{}/boards.json'.format(engine, market)

    @staticmethod
    def get_securities(engine: str, market: str, board: str):
        """
        Получить dataset securities по engine-market-board
        :param engine: Название engine (string)
        :param market: Название market (string)
        :param board: Название boards (string)
        :return: securities (JSON)
        """

        return 'http://iss.moex.com/iss/engines/{}/markets/{}/boards/{}/securities.json'.format(engine, market, board)

    @staticmethod
    def get_history(engine: str, market: str, board: str, date: str):
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
