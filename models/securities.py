from models.main import Main


class Securities(Main):
    def __init__(self):
        self.name = "securities"  # Название таблицы

    async def start(self, session):
        while True:
            boards = await self.download(session, "boards")  # Данные с сервера ("boards")
            server_data = await self.download(session, self.name)  # Целевые данные с сервера для проверки

            # Загрузка securities по каждому board
            for board in boards.get("boards"):
                async with session.get(self.get_securities(board.get("engine_name"), board.get("market_name"),
                                                           board.get("board_id"))) as resp:
                    securities = await resp.json()  # Данные с биржи

                    for data in securities.get(self.name).get("data"):
                        # Объединение массивов (столбцов и данных) в словарь
                        new_data = dict(zip(securities.get(self.name).get("columns"), data))

                        # Проще создать новый словарь, чем изменить исходный
                        new_new_data = {}

                        # Переименование ключей
                        if new_data.get("SECID") is not None:
                            new_new_data["sec_id"] = new_data.pop("SECID")
                        if new_data.get("BOARDID") is not None:
                            new_new_data["board_id"] = new_data.pop("BOARDID")

                        # Добавление информации
                        new_new_data["market_name"] = board.get("market_name")
                        new_new_data["engine_name"] = board.get("engine_name")

                        # Здась нет id у исходного dataset. Удалять нечего.
                        # Проверка на совпадение и загрузка в БД
                        await self.post(session, server_data, self.name, new_new_data)
