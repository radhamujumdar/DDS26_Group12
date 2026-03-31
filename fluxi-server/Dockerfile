FROM python:3.12-slim

WORKDIR /home/fluxi-server

COPY packages/fluxi-engine /home/fluxi-server/packages/fluxi-engine
COPY fluxi-server /home/fluxi-server/fluxi-server

RUN pip install --no-cache-dir -r /home/fluxi-server/fluxi-server/requirements.txt /home/fluxi-server/packages/fluxi-engine

WORKDIR /home/fluxi-server/fluxi-server

EXPOSE 8001

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]
