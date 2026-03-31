FROM python:3.12-slim

WORKDIR /home/flask-app

COPY stock/requirements.txt /home/flask-app/stock/requirements.txt
COPY packages/fluxi-sdk /home/flask-app/packages/fluxi-sdk
COPY packages/fluxi-engine /home/flask-app/packages/fluxi-engine

RUN pip install --no-cache-dir -r /home/flask-app/stock/requirements.txt /home/flask-app/packages/fluxi-sdk /home/flask-app/packages/fluxi-engine

COPY stock /home/flask-app/stock

WORKDIR /home/flask-app/stock

EXPOSE 5000
