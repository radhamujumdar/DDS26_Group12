FROM python:3.12-slim

WORKDIR /home/flask-app

COPY payment/requirements.txt /home/flask-app/payment/requirements.txt
COPY packages/fluxi-sdk /home/flask-app/packages/fluxi-sdk

RUN pip install --no-cache-dir -r /home/flask-app/payment/requirements.txt /home/flask-app/packages/fluxi-sdk

COPY payment /home/flask-app/payment

WORKDIR /home/flask-app/payment

EXPOSE 5000
