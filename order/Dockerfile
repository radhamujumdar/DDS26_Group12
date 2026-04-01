FROM python:3.12-slim

WORKDIR /home/flask-app

COPY order/requirements.txt /home/flask-app/order/requirements.txt
COPY packages/shop-common /home/flask-app/packages/shop-common
COPY packages/fluxi-sdk /home/flask-app/packages/fluxi-sdk
COPY packages/fluxi-engine /home/flask-app/packages/fluxi-engine

RUN pip install --no-cache-dir -r /home/flask-app/order/requirements.txt /home/flask-app/packages/shop-common /home/flask-app/packages/fluxi-sdk /home/flask-app/packages/fluxi-engine

COPY order /home/flask-app/order

WORKDIR /home/flask-app

EXPOSE 5000
