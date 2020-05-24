import datetime
import logging

import aiohttp

from db_road.rest import REST


class History(REST):
    def __init__(self):
        super().__init__()
        self.name: str = "securities_moex"  # Название таблицы
        self.valid_board: bool = True

    def conversion(self, source: dict, data: dict):
        # Объединение массивов (столбцов и данных) в словарь
        new_data: dict = dict(zip(source.get("history").get("columns"), data))

        # Проще создать новый словарь, чем изменить исходный
        new_new_data: dict = {"board_id": new_data.pop("BOARDID"), "trade_date": new_data.pop("TRADEDATE"),
                              "short_name": new_data.pop("SHORTNAME"), "sec_id": new_data.pop("SECID"),
                              "open": new_data.pop("OPEN"), "low": new_data.pop("LOW"), "high": new_data.pop("HIGH"),
                              "close": new_data.pop("CLOSE")}

        # Переименование ключей
        if new_data.get("VOLUME") is not None:
            new_new_data["volume"] = new_data.pop("VOLUME")
        # Прерываем итерацию если нет "volume"
        else:
            self.valid_board: bool = False
            return

        return new_new_data

    async def start(self, session: aiohttp):
        while True:
            sub_sources: dict = await self.get(session, "boards")  # Данные с сервера ("boards")
            server_data: dict = await self.get(session, self.name)  # Целевые данные с сервера для проверки

            # Загрузка истории по каждому board
            for sub_source in sub_sources.get("boards"):
                today: datetime = datetime.datetime.today()
                today: datetime = today - datetime.timedelta(1)
                today_str: str = datetime.datetime.strftime(today, "%Y-%m-%d")
                count: int = 0  # Счетчик пропусков (неторговых дней)
                self.valid_board: bool = True  # Наличие у dataset столбца "volume"

                # Продолжает работу только если есть "volume" в данном boards. Иначе - пропускаем итерацию
                if self.valid_board:
                    # Пока счетчик не превышает 100 дней пропусков: работаем дальше. Иначе - прерываем итерацию.
                    while count < 100:
                        # Если у dataset нет "volume": прерываем итерацию.
                        if self.valid_board is False:
                            break

                        url: str = self.get_history(sub_source.get("engine_name"),
                                                    sub_source.get("market_name"),
                                                    sub_source.get("board_id"), today_str)

                        try:
                            async with session.get(url) as resp:
                                logging.debug("GET from {}, status {}".format(url, resp.status))

                                source: dict = await resp.json()  # Данные с биржи

                                # Если неторговый день: записывааем день в счетчик и пропускаем итерацию. Иначе -
                                # обнуляем счетчик и работаем дальше
                                if len(source.get("history").get("data")) == 0:
                                    count += 1
                                    today: datetime = today - datetime.timedelta(1)
                                    today_str: str = datetime.datetime.strftime(today, "%Y-%m-%d")
                                    continue
                                else:
                                    today: datetime = today - datetime.timedelta(1)
                                    today_str: str = datetime.datetime.strftime(today, "%Y-%m-%d")
                                    count: int = 0

                                for data in source.get("history").get("data"):
                                    # Здась нет id у исходного dataset. Удалять нечего.
                                    # Проверка на совпадение и загрузка в БД
                                    await self.post(session, server_data, self.conversion(source, data), self.name)
                        except aiohttp.ClientConnectorError:
                            logging.error("Cannot connect to host {}".format(url))
