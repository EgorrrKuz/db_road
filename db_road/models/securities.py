from db_road.main import Main
import aiohttp
import logging


class Securities(Main):
    def __init__(self):
        super().__init__()
        self.name: str = "securities"  # Название таблицы

    def conversion(self, sub_source, source, data):
        # Объединение массивов (столбцов и данных) в словарь
        new_data: dict = dict(zip(source.get(self.name).get("columns"), data))

        # Проще создать новый словарь, чем изменить исходный
        new_new_data: dict = {}

        # Переименование ключей
        if new_data.get("SECID") is not None:
            new_new_data["sec_id"] = new_data.pop("SECID")
        if new_data.get("BOARDID") is not None:
            new_new_data["board_id"] = new_data.pop("BOARDID")

        # Добавление информации
        new_new_data["market_name"] = sub_source.get("market_name")
        new_new_data["engine_name"] = sub_source.get("engine_name")

        return new_new_data

    async def start(self, session):
        while True:
            sub_sources: dict = await self.download(session, "boards")  # Данные с сервера ("boards")
            server_data: dict = await self.download(session, self.name)  # Целевые данные с сервера для проверки

            # Загрузка securities по каждому board
            if sub_sources is not None:
                for sub_source in sub_sources.get("boards"):
                    # URL откуда парсим данные
                    url: str = self.get_securities(
                        sub_source.get("engine_name"),
                        sub_source.get("market_name"),
                        sub_source.get("board_id"))

                    await self.plunk(session, url, self.name, sub_source, server_data, self.conversion)
