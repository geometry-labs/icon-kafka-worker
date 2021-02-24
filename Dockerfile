FROM python:3.7 as compile
ENV PYTHONPATH "${PYTHONPATH}:/usr/src/app"
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install --user --no-cache-dir -r requirements.txt

FROM python:3.7-slim as base
ENV PYTHONPATH "${PYTHONPATH}:/usr/src/app"
COPY --from=compile /root/.local /root/.local
WORKDIR /usr/src/app
COPY . .
ENV PATH=/root/.local/bin:$PATH

FROM base as prod
CMD [ "python", "./iconkafkaworker/main.py" ]

FROM base as test