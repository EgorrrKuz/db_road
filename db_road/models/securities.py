import aiohttp
from rest import REST


class Securities(REST):
    def __init__(self):
        super().__init__()
        self.name: str = "securities"  # Название таблицы

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

        # It’s easier to create a new dictionary than to change the original
        new_new_data: dict = {"secId": new_data.pop("SECID"), "shortName": new_data.pop("SHORTNAME"),
                              "boardId": new_data.pop("BOARDID"), "marketName": sub_source.get("marketName"),
                              "engineName": sub_source.get("engineName")}

        return new_new_data

    async def start(self, session: aiohttp):
        """
        Start data loading

        :param session: Session
        """

        while True:
            sub_sources: dict = await self.get(session, "boards")   # Data from the server ("boards")
            server_data: dict = await self.get(session, self.name)  # Target data from the server for verification

            # Download "securities" for each "boards"
            if sub_sources is not None:
                for sub_source in sub_sources:
                    # URL from where we parse data
                    url: str = self.get_securities(
                        sub_source.get("engineName"),
                        sub_source.get("marketName"),
                        sub_source.get("boardId"))

                    # Enumeration and POST data
                    await self.plunk(session, url, self.name, sub_source, server_data, self.conversion)
