FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY data-collector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/ ./proto/

RUN python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/user_service.proto && \
    python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/data_collector_service.proto

COPY data-collector/ .

CMD ["python", "app.py"]
