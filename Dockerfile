FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py single_message_consumer.py batch_message_consumer.py kafka_message.py ./

RUN groupadd -r appgroup && useradd -r -g appgroup appuser
RUN chown -R appuser:appgroup /app
USER appuser