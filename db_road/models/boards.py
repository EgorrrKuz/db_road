import aiohttp
from db_road.rest import REST


class Boards(REST):
    def __init__(self):
        super().__init__()
        self.name: str = "boards"  # Название таблицы

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

        # Выбор только торгвых(is_traded) площадок (boards)
        if new_data.get("is_traded") == 1:
            # Переименование ключей
            new_data["board_id"] = new_data.pop("boardid")

            # Добавление информации
            new_data["engine_name"] = sub_source.get("engine_name")
            new_data["market_name"] = sub_source.get("name")
            new_data.pop("is_traded")

            new_data.pop("id")  # Удаление столбца id

            return new_data

        return

    async def start(self, session: aiohttp):
        """
        Начать загрузку данных

        :param session: session
        """

        while True:
            sub_sources: dict = await self.get(session, "markets")  # Данные с сервера ("markets")
            server_data: dict = await self.get(session, self.name)  # Целевые данные с сервера для проверки

            if sub_sources is not None:
                # Загрузка source по каждому sub_source
                for sub_source in sub_sources.get("markets"):
                    # URL откуда парсим данные
                    url: str = self.get_boards(sub_source.get("engine_name", ), sub_source.get("name", ))

                    await self.plunk(session, url, self.name, sub_source, server_data, self.conversion)
