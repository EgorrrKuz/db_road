import datetime
from db_road.main import Main
import aiohttp
import logging


class History(Main):
    def __init__(self):
        super().__init__()
        self.name: str = "securities_moex"  # Название таблицы

    async def start(self, session):
        while True:
            boards: dict = await self.download(session, "boards")           # Данные с сервера ("boards")
            server_data: dict = await self.download(session, self.name)     # Целевые данные с сервера для проверки

            # Загрузка истории по каждому board
            if boards is not None:
                for board in boards.get("boards"):
                    # Дата торгов
                    today: datetime = datetime.datetime.today()
                    today: datetime = today - datetime.timedelta(1)
                    today_str: str = datetime.datetime.strftime(today, "%Y-%m-%d")

                    count: int = 0              # Счетчик пропусков (неторговых дней)
                    valid_board: bool = True    # Наличие у dataset столбца "volume"

                    # Продолжает работу только если есть "volume" в данном boards. Иначе - пропускаем итерацию
                    if valid_board:
                        # Пока счетчик не превышает 100 дней пропусков: работаем дальше. Иначе - прерываем итерацию.
                        while count < 100:
                            # Если у dataset нет "volume": прерываем итерацию.
                            if valid_board is False:
                                break

                            # URL откуда парсим данные
                            url: str = self.get_history(
                                board.get("engine_name"),
                                board.get("market_name"),
                                board.get("board_id"), today_str)

                            try:
                                async with session.get(url) as resp:
                                    logging.debug("GET from {}, status {}".format(url, resp.status))

                                    security: dict = await resp.json()  # Данные с биржи

                                    # Если неторговый день: записывааем день в счетчик и пропускаем итерацию. Иначе -
                                    # обнуляем счетчик и работаем дальше
                                    if len(security.get("history").get("data")) == 0:
                                        count += 1
                                        today: datetime = today - datetime.timedelta(1)
                                        today_str: str = datetime.datetime.strftime(today, "%Y-%m-%d")
                                        continue
                                    else:
                                        today: datetime = today - datetime.timedelta(1)
                                        today_str: str = datetime.datetime.strftime(today, "%Y-%m-%d")
                                        count: int = 0

                                    for data in security.get("history").get("data"):
                                        # Объединение массивов (столбцов и данных) в словарь
                                        new_data: dict = dict(zip(security.get("history").get("columns"), data))

                                        # Проще создать новый словарь, чем изменить исходный
                                        new_new_data: dict = {}

                                        # Переименование ключей
                                        if new_data.get("BOARDID") is not None:
                                            new_new_data["board_id"] = new_data.pop("BOARDID")
                                        if new_data.get("TRADEDATE") is not None:
                                            new_new_data["trade_date"] = new_data.pop("TRADEDATE")
                                        if new_data.get("SHORTNAME") is not None:
                                            new_new_data["short_name"] = new_data.pop("SHORTNAME")
                                        if new_data.get("SECID") is not None:
                                            new_new_data["sec_id"] = new_data.pop("SECID")
                                        if new_data.get("OPEN") is not None:
                                            new_new_data["open"] = new_data.pop("OPEN")
                                        if new_data.get("LOW") is not None:
                                            new_new_data["low"] = new_data.pop("LOW")
                                        if new_data.get("HIGH") is not None:
                                            new_new_data["high"] = new_data.pop("HIGH")
                                        if new_data.get("CLOSE") is not None:
                                            new_new_data["close"] = new_data.pop("CLOSE")
                                        if new_data.get("VOLUME") is not None:
                                            new_new_data["volume"] = new_data.pop("VOLUME")
                                        # Прерываем итерацию если нет "volume"
                                        else:
                                            valid_board: bool = False
                                            break

                                        # Здась нет id у исходного dataset. Удалять нечего.
                                        # Проверка на совпадение и загрузка в БД
                                        await self.post(session, server_data, self.name, new_new_data)
                            except aiohttp.ClientConnectorError:
                                logging.error("Cannot connect to host {}".format(url))
