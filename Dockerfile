FROM python:3.10

WORKDIR /app

COPY requirements.txt /app
COPY dev-requirements.txt /app

RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install -r dev-requirements.txt
