from db_road.main import Main
import aiohttp
import logging


class Markets(Main):
    def __init__(self):
        super().__init__()
        self.name: str = "markets"

    def conversion(self, sub_source, source, data):
        # Объединение массивов (столбцов и данных) в словарь
        new_data: dict = dict(zip(source.get(self.name).get("columns"), data))

        # Переименование ключей
        new_data["name"] = new_data.pop("NAME")
        new_data["engine_name"] = sub_source.get("name")

        new_data.pop("id")  # Удаление столбца id

        return new_data

    async def start(self, session):
        while True:
            sub_sources: dict = await self.download(session, "engines")  # Данные с сервера ("engines")
            server_data: dict = await self.download(session, self.name)  # Целевые данные с сервера для проверки

            # Загрузка market по каждому engine
            if sub_sources is not None:
                for sub_source in sub_sources.get("engines"):
                    # URL откуда парсим данные
                    url: str = self.get_markets(sub_source.get("name"))

                    await self.plunk(session, url, self.name, sub_source, server_data, self.conversion)
