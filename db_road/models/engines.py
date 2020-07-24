import asyncio

import aiohttp
import logging
from rest import REST


class Engines(REST):
    def __init__(self):
        super().__init__()
        self.name: str = "engines"  # Table name

    def conversion(self, source: dict, data: dict):
        """
        Converting an object to the correct view for the DB

        :param source: Source data
        :param data: Uploaded data
        :return: New view of the object
        """

        # Combining arrays (columns and data) into a dictionary
        new_data: dict = dict(zip(source.get(self.name).get("columns"), data))
        new_data.pop("id")  # Delete id column

        return new_data

    async def start(self, session: aiohttp):
        """
        Start data loading

        :param session: Session
        """

        while True:
            server_data: dict = await self.get(session, self.name)     # Target data from the server for verification
            url: str = self.get_engines()                              # URL from where we parse data

            if server_data is not None:
                # Download data from the exchange
                try:
                    async with session.get(url) as resp:
                        logging.debug("GET from {}, status {}".format(url, resp.status))

                        source: dict = await resp.json()  # Exchange data

                        # Enumeration and POST data
                        for data in source.get("engines").get("data"):
                            # Matching and loading into the DB
                            await self.post(session, server_data, self.conversion(source, data), self.name)
                except aiohttp.ClientConnectorError:
                    logging.error("Cannot connect to host {}".format(url))
                except asyncio.exceptions.TimeoutError:
                    logging.error("TimeoutError on {}".format(url))
