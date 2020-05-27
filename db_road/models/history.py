import datetime
import logging

import aiohttp

from db_road.rest import REST


class History(REST):
    def __init__(self):
        super().__init__()
        self.name: str = "securities_moex"  # Table name
        self.valid_board: bool = True

    def conversion(self, source: dict, data: dict):
        """
        Converting an object to the correct view for the DB

        :param source: Source data
        :param data: Uploaded data
        :return: New view of the object
        """

        # Combining arrays (columns and data) into a dictionary
        new_data: dict = dict(zip(source.get("history").get("columns"), data))

        # It’s easier to create a new dictionary than to change the original
        new_new_data: dict = {"board_id": new_data.pop("BOARDID"), "trade_date": new_data.pop("TRADEDATE"),
                              "short_name": new_data.pop("SHORTNAME"), "sec_id": new_data.pop("SECID"),
                              "open": new_data.pop("OPEN"), "low": new_data.pop("LOW"), "high": new_data.pop("HIGH"),
                              "close": new_data.pop("CLOSE")}

        # Rename keys
        if new_data.get("VOLUME") is not None:
            new_new_data["volume"] = new_data.pop("VOLUME")
        # Abort the iteration if there is no "volume"
        else:
            self.valid_board: bool = False
            return

        return new_new_data

    async def start(self, session: aiohttp):
        """
        Start data loading

        :param session: Session
        """

        while True:
            sub_sources: dict = await self.get(session, "boards")   # Data from the server ("boards")
            server_data: dict = await self.get(session, self.name)  # Target data from the server for verification

            # Download history for each board
            for sub_source in sub_sources.get("boards"):
                today: datetime = datetime.datetime.today()
                today: datetime = today - datetime.timedelta(1)
                today_str: str = datetime.datetime.strftime(today, "%Y-%m-%d")
                count: int = 0  # Badge counter (non-trading days)
                self.valid_board: bool = True  # The dataset has a "volume" column

                # It continues to work only if there is a "volume" in this boards. Otherwise, skip the iteration
                if self.valid_board:
                    # While the counter does not exceed 100 days of passes: we work further.
                    # Otherwise, we interrupt the iteration.
                    while count < 100:
                        # If dataset does not have "volume": abort the iteration.
                        if self.valid_board is False:
                            break

                        url: str = self.get_history(sub_source.get("engine_name"),
                                                    sub_source.get("market_name"),
                                                    sub_source.get("board_id"), today_str)

                        try:
                            async with session.get(url) as resp:
                                logging.debug("GET from {}, status {}".format(url, resp.status))

                                source: dict = await resp.json()  # Exchange data

                                # If a non-trading day: write the day in the counter and skip the iteration.
                                # Otherwise - reset the counter and work on.
                                if len(source.get("history").get("data")) == 0:
                                    count += 1
                                    today: datetime = today - datetime.timedelta(1)
                                    today_str: str = datetime.datetime.strftime(today, "%Y-%m-%d")
                                    continue
                                else:
                                    today: datetime = today - datetime.timedelta(1)
                                    today_str: str = datetime.datetime.strftime(today, "%Y-%m-%d")
                                    count: int = 0

                                for data in source.get("history").get("data"):
                                    # Here the id dataset has no id. There is nothing to delete.
                                    # Matching and loading into the database
                                    await self.post(session, server_data, self.conversion(source, data), self.name)
                        except aiohttp.ClientConnectorError:
                            logging.error("Cannot connect to host {}".format(url))
