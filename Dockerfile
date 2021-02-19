FROM python:3.7 as base

ENV PYTHONPATH "${PYTHONPATH}:/usr/src/app"

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./iconkafkaworker/main.py" ]

FROM base as prod
