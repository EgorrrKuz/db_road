import aiohttp
from db_road.rest import REST


class Securities(REST):
    def __init__(self):
        super().__init__()
        self.name: str = "securities"  # Название таблицы

    def conversion(self, sub_source: dict, source: dict, data: dict):
        """
        Преобразование объекта в правильный вид для БД

        :param sub_source: Вспомогательный источник
        :param source: Источник
        :param data: Загруженные данные
        :return: Новыей вид объекта
        """

        # Объединение массивов (столбцов и данных) в словарь
        new_data: dict = dict(zip(source.get(self.name).get("columns"), data))

        # Проще создать новый словарь, чем изменить исходный
        new_new_data: dict = {"sec_id": new_data.pop("SECID"), "board_id": new_data.pop("BOARDID"),
                              "market_name": sub_source.get("market_name"),
                              "engine_name": sub_source.get("engine_name")}

        # Переименование ключей

        # Добавление информации

        return new_new_data

    async def start(self, session: aiohttp):
        """
        Начать загрузку данных

        :param session: session
        """

        while True:
            sub_sources: dict = await self.get(session, "boards")  # Данные с сервера ("boards")
            server_data: dict = await self.get(session, self.name)  # Целевые данные с сервера для проверки

            # Загрузка securities по каждому board
            if sub_sources is not None:
                for sub_source in sub_sources.get("boards"):
                    # URL откуда парсим данные
                    url: str = self.get_securities(
                        sub_source.get("engine_name", ),
                        sub_source.get("market_name", ),
                        sub_source.get("board_id", ))

                    await self.plunk(session, url, self.name, sub_source, server_data, self.conversion)
