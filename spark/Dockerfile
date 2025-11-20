FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/app

COPY requirements_spark.txt .
RUN pip install --no-cache-dir -r requirements_spark.txt

COPY spark_cdc_etl.py .

CMD ["python", "spark_cdc_etl.py"]