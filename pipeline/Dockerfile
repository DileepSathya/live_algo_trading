FROM python:3.12-slim
WORKDIR /app
COPY pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestion_normalization/ ingestion_normalization/
COPY pipeline/*.py pipeline/
