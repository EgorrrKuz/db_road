from models.main import Main


class Boards(Main):
    def __init__(self):
        self.name = "boards"  # Название таблицы

    async def start(self, session):
        while True:
            markets = await self.download(session, "markets")  # Данные с сервера ("markets")
            server_data = await self.download(session, self.name)  # Целевые данные с сервера для проверки

            # Загрузка board по каждому market
            for market in markets.get("markets"):
                async with session.get(self.get_boards(market.get("engine_name"), market.get("name"))) as resp:
                    boards = await resp.json()  # Данные с биржи

                    for data in boards.get(self.name).get("data"):
                        # Объединение массивов (столбцов и данных) в словарь
                        new_data = dict(zip(boards.get(self.name).get("columns"), data))

                        # Выбор только торгвых(is_traded) площадок (boards)
                        if new_data.get("is_traded") == 1:
                            # Переименование ключей
                            new_data["board_id"] = new_data.pop("boardid")

                            # Добавление информации
                            new_data["engine_name"] = market.get("engine_name")
                            new_data["market_name"] = market.get("name")
                            new_data.pop("is_traded")

                            new_data.pop("id")  # Удаление столбца id

                            # Проверка на совпадение и загрузка в БД
                            await self.post(session, server_data, self.name, new_data)
