FROM python:3.11-slim
WORKDIR /app
COPY docker/serving/requirements-serving.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY processing /app/processing
CMD ["python","-m","processing.serving.api"]
