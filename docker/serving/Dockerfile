FROM python:3.11-slim

WORKDIR /app

# thriftpy2 builds its C extension when no prebuilt wheel is available.
# Keep the compiler in the image only for dependency installation.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY docker/serving/requirements-serving.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY processing /app/processing

RUN apt-get purge -y build-essential \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

CMD ["python","-m","processing.serving.api"]
