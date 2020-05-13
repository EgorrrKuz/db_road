from models.main import Main


class Markets(Main):
    def __init__(self):
        self.name = "markets"

    async def start(self, session):
        while True:
            engines = await self.download(session, "engines")  # Данные с сервера ("engines")
            server_data = await self.download(session, self.name)  # Целевые данные с сервера для проверки

            # Загрузка market по каждому engine
            for _name in engines.get("engines"):
                # Загрузка данных с биржи
                async with session.get(self.get_markets(_name.get("name"))) as resp:
                    markets = await resp.json()  # Данные с биржи

                    # Перебор массива данных и преобразование
                    for data in markets.get(self.name).get("data"):
                        # Объединение массивов (столбцов и данных) в словарь
                        new_data = dict(zip(markets.get(self.name).get("columns"), data))

                        # Переименование ключей
                        new_data["name"] = new_data.pop("NAME")
                        new_data["engine_name"] = _name.get("name")

                        new_data.pop("id")  # Удаление столбца id

                        # Проверка на совпадение и загрузка в БД
                        await self.post(session, server_data, self.name, new_data)
