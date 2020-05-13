# Микросервис db_road.

Загрузка данных с биржи на сервер.
Подключается к [REST API](https://github.com/EgorrrKuz/tradeBot/tree/master/AIServer/REST_API).

Загрузка исторических данных возможна только на дневном TimeFrame.

### Настрока подключения

В файле [`models/main`](https://github.com/EgorrrKuz/tradeBot/blob/master/AIServer/db_road/models/main.py) настраиваем подключение:

```angular2
server_url = "rest" URL для подключения к REST API
server_port = "8080" Потр на котором работает REST API
```

### Работа с биржей
Елси изменятся адреса для загрузки данных  с биржи: изменить их в своих методах в 
[`models/main`](https://github.com/EgorrrKuz/tradeBot/blob/master/AIServer/db_road/models/main.py):

```angular2
def get_engines():
    """
    Получить dataset engines
    :return: engines (JSON)
    """

    return 'http://iss.moex.com/iss/engines.json'
```
