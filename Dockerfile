FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .
COPY domain ./domain
COPY migrations ./migrations
COPY repositories ./repositories
COPY services ./services
COPY data ./data

CMD ["python", "-u", "app.py"]
