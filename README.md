# Microservice db_road.
[![BCH compliance](https://bettercodehub.com/edge/badge/EgorrrKuz/db_road?branch=master)](https://bettercodehub.com/)

The microservice is intended for parsing data from a cloud json file into its database. Microservice fully works on aiohttp

Works directly with REST API prepared for target data.

The necessary methods for working with REST are created in the [`rest`](https://github.com/EgorrrKuz/db_road/blob/master/db_road/main.py) file.

## Connection setup
Connection setup is carried out in the [`main`](https://github.com/EgorrrKuz/db_road/blob/master/db_road/rest.py) file:
```python3
server_url: str = "rest_url"  # URL to connect to the REST API
server_port: str = "8080"     # Pot on which REST API works
```

The URL for loading data is indicated in the dictionary (also in [`main`](https://github.com/EgorrrKuz/db_road/blob/master/db_road/main.py))
```python3
self.dataset_server: dict = {
            "engines": "http://" + self.server_url + ":" + self.server_port + "/engines",
            "markets": "http://" + self.server_url + ":" + self.server_port + "/markets",
            "boards": "http://" + self.server_url + ":" + self.server_port + "/boards",
            "securities": "http://" + self.server_url + ":" + self.server_port + "/securities",
            "securities_moex": "http://" + self.server_url + ":" + self.server_port + "/securities_moex"
        }
```

Connection to the target server is also configured in the [`main`](https://github.com/EgorrrKuz/db_road/blob/master/db_road/main.py)
 in functions:
 ```python3
def get_engines():
    """
    Get dataset "engines"
    :return: engines (JSON)
    """

    return "http://iss.moex.com/iss/engines.json"
```

## Model customization
Model configuration is carried out in the [`models`](https://github.com/EgorrrKuz/db_road/blob/master/db_road/models/) directory.
A file is created and in the "start" method the model is set up. If you need to format the data: this is done in the "conversion" method.
Next, the "start" method is launched in the [`app`](https://github.com/EgorrrKuz/db_road/blob/master/db_road/app.py) file in a separate thread.
 This is necessary for the speed of the microservice