FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

ENV AIRFLOW_HOME=/app/airflow
# Should that based on the environment. localhost for dev
ENV DATABASE_HOST=host.docker.internal 

COPY ./entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
