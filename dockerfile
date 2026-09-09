FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    DLT_DATA_DIR=/tmp/dlt

WORKDIR /app

COPY requirements-pipeline.txt ./
RUN pip install --no-cache-dir -r requirements-pipeline.txt

COPY scripts/ /app/

RUN useradd --create-home --uid 50000 pipeline
USER pipeline

ENTRYPOINT ["python"]
CMD ["/app/api_to_minio.py"]
