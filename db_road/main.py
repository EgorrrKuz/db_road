class Main:
    def __init__(self):
        self.server_url: str = "rest"  # To run on the local server
        # server_url: str = "rest"          # To run in a docker container
        self.server_port: str = "80"      # Port running REST API

        # Links for parsing data from the server
        self.dataset_server: dict = {
            "engines": "http://" + self.server_url + ":" + self.server_port + "/api/engines",
            "markets": "http://" + self.server_url + ":" + self.server_port + "/api/markets",
            "boards": "http://" + self.server_url + ":" + self.server_port + "/api/boards",
            "securities": "http://" + self.server_url + ":" + self.server_port + "/api/securities",
            "history": "http://" + self.server_url + ":" + self.server_port + "/api/history"
        }

    # Methods for obtaining data from the exchange
    @staticmethod
    def get_engines():
        """
        Get dataset "engines"
        :return: engines (JSON)
        """

        return "http://iss.moex.com/iss/engines.json"

    @staticmethod
    def get_markets(engine: str):
        """
        Get dataset "markets" from "engine"
        :param engine: Name engine (string)
        :return: markets (JSON)
        """

        return "http://iss.moex.com/iss/engines/{}/markets.json".format(engine)

    @staticmethod
    def get_boards(engine: str, market: str):
        """
        Get dataset "markets" from "engine-market"
        :param engine: Name engine (string)
        :param market: Name market (string)
        :return: boards (JSON)
        """

        return "http://iss.moex.com/iss/engines/{}/markets/{}/boards.json".format(engine, market)

    @staticmethod
    def get_securities(engine: str, market: str, board: str):
        """
        Get dataset "securities" from "engine-market-board"
        :param engine: Name engine (string)
        :param market: Name market (string)
        :param board: Name boards (string)
        :return: securities (JSON)
        """

        return "http://iss.moex.com/iss/engines/{}/markets/{}/boards/{}/securities.json".format(engine, market, board)

    @staticmethod
    def get_history(engine: str, market: str, board: str, date: str):
        """
        Get dataset "history" from "engine-market-board"

        :param engine: Name engine (string)
        :param market: Name market (string)
        :param board: Name boards (string)
        :param date: Date (string: "%Y-%d-%m")
        :return: securities (JSON)
        """

        return "http://iss.moex.com/iss/history/engines/{}/markets/{}/boards/{}/securities.json?date={}".format(engine,
                                                                                                                market,
                                                                                                                board,
                                                                                                                date)
