import aiohttp
from db_road.rest import REST


class Boards(REST):
    def __init__(self):
        super().__init__()
        self.name: str = "boards"  # Table name

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

        # Selection of only trading (is_traded) platforms (boards)
        if new_data.get("is_traded") == 1:
            # Rename keys
            new_data["board_id"]: dict = new_data.pop("boardid")

            # Adding Information
            new_data["engine_name"]: dict = sub_source.get("engine_name")
            new_data["market_name"]: dict = sub_source.get("name")
            new_data.pop("is_traded")

            new_data.pop("id")  # Delete id column

            return new_data

        return

    async def start(self, session: aiohttp):
        """
        Start data loading

        :param session: Session
        """

        while True:
            sub_sources: dict = await self.get(session, "markets")  # Data from the server ("markets")
            server_data: dict = await self.get(session, self.name)  # Target data from the server for verification

            if sub_sources is not None:
                # Download "boards" for each "markets"
                for sub_source in sub_sources.get("markets"):
                    # URL from where we parse data
                    url: str = self.get_boards(sub_source.get("engine_name"), sub_source.get("name", ))

                    # Enumeration and POST data
                    await self.plunk(session, url, self.name, sub_source, server_data, self.conversion)
