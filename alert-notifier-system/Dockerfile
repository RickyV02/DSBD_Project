FROM python:3.11-slim

WORKDIR /app

COPY alert-notifier-system/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY alert-notifier-system/ .

CMD ["python", "app.py"]
