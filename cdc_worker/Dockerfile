FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cdc_worker.py .

CMD ["python", "-u", "cdc_worker.py"]
