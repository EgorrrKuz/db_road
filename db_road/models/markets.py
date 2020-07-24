import aiohttp
from rest import REST


class Markets(REST):
    def __init__(self):
        super().__init__()
        self.name: str = "markets"

    def conversion(self, sub_source: dict, source: dict, data: dict):
        """
        Converting an object to the correct view for the DB

        :param sub_source: Sub source
        :param source: Source data
        :param data: Uploaded data
        :return: New view of the object
        """

        # Combining arrays (columns and data) into a dictionary
        new_data: dict = dict(zip(source.get(self.name).get("columns"), data))

        # Rename keys
        new_data["name"] = new_data.pop("NAME")
        new_data["engineName"] = sub_source.get("name")

        new_data.pop("id")  # Delete "id" column

        return new_data

    async def start(self, session: aiohttp):
        """
        Start data loading

        :param session: Session
        """

        while True:
            sub_sources: dict = await self.get(session, "engines")  # Data from the server ("engines")
            server_data: dict = await self.get(session, self.name)  # Target data from the server for verification

            # Download "market" for each "engine"
            if sub_sources is not None:
                for sub_source in sub_sources:
                    # URL from where we parse data
                    url: str = self.get_markets(sub_source.get("name"))

                    # Enumeration and POST data
                    await self.plunk(session, url, self.name, sub_source, server_data, self.conversion)
