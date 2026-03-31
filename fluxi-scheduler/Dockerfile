FROM python:3.12-slim

WORKDIR /home/fluxi-scheduler

COPY packages/fluxi-engine /home/fluxi-scheduler/packages/fluxi-engine
COPY fluxi-scheduler /home/fluxi-scheduler/fluxi-scheduler

RUN pip install --no-cache-dir -r /home/fluxi-scheduler/fluxi-scheduler/requirements.txt /home/fluxi-scheduler/packages/fluxi-engine

WORKDIR /home/fluxi-scheduler/fluxi-scheduler

CMD ["python", "main.py"]
