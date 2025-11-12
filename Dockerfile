FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ scripts/
COPY dags/ dags/

CMD ["python", "scripts/fetch_weather_postgres.py"]
