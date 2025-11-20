FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cdc_producer.py writer_simulator.py ./

CMD ["python", "cdc_producer.py"]