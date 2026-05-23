# producer/Dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python-ng==2.2.3 \
    fastavro==1.9.4 \
    requests==2.31.0 \
    python-dotenv==1.0.1

COPY mock_producer.py .

CMD ["python", "-u", "mock_producer.py"]