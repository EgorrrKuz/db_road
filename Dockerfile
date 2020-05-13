FROM python:3.8

ENV PYTHONUBUFFERED 1

WORKDIR /app/db_road

COPY requirements.txt /app/db_road
RUN pip3 install --upgrade pip -r requirements.txt

COPY . /app/db_road

CMD python app.py