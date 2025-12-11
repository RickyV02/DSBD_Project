FROM python:3.11-slim

WORKDIR /app

COPY alert-system/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY alert-system/ .

CMD ["python", "app.py"]
