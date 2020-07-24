import asyncio
import json
import logging
import aiohttp
from main import Main


class REST(Main):
    @staticmethod
    def coincidences(server_data: dict, data: dict, name: str):
        """
        Find matches in dataset

        :param server_data: Data from the server
        :param data: New data
        :param name: Dataset name
        :return: Coincidence
        """

        coincidence: bool = False

        # Comparison with server data
        if data is not None:
            for _data in server_data:
                if _data.get("id", ) is not None:
                    _data.pop("id")

                if data == _data:
                    coincidence: bool = True

        return coincidence

    async def post(self, session: aiohttp, server_data: dict, data: dict, name: str):
        """
        Matching and loading into the database

        :param session: Session
        :param server_data: Data from the server
        :param name: Dataset name
        :param data: Downloadable data
        """

        url: str = self.dataset_server.get(name)  # URL where we load data

        # If no match is found: POST to server
        if data is not None and self.coincidences(server_data, data, name) is False:
            try:
                async with session.post(url=url, json=data) as resp:
                    logging.debug("POST to {}, status {}".format(url, resp.status))
            except aiohttp.ClientConnectorError:
                logging.error("Cannot connect to host {}".format(url))

    async def get(self, session: aiohttp, name: str):
        """
        Download data from server

        :param session: Session
        :param name: Dataset name
        :return: Data from the server (JSON)
        """

        url: str = self.dataset_server.get(name)  # URL from where we parse data

        # GET from the server
        try:
            async with session.get(url) as resp:
                logging.debug("GET from {}, status {}".format(url, resp.status))

                return await resp.json()
        except aiohttp.ClientConnectorError:
            logging.error("Cannot connect to host {}".format(url))
        except asyncio.exceptions.TimeoutError:
            logging.error("TimeoutError on {}".format(url))

    async def plunk(self, session: aiohttp, url: str, name: str, sub_source: dict, server_data: dict,
                    new_data: classmethod):
        try:
            # Download data from the exchange
            async with session.get(url) as resp:
                logging.debug("GET from {}, status {}".format(url, resp.status))

                source: dict = await resp.json()  # Exchange data

                # Dataset enumeration and conversion
                for data in source.get(name).get("data"):
                    # Matching and loading into the database
                    await self.post(session, server_data, new_data(sub_source, source, data),  name)
        except aiohttp.ClientConnectorError:
            logging.error("Cannot connect to host {}".format(url))
        except asyncio.exceptions.TimeoutError:
            logging.error("TimeoutError on {}".format(url))
