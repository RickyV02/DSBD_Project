FROM python:3.11-slim

WORKDIR /app

COPY user-manager/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/ ./proto/

RUN python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/user_service.proto
RUN python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/data_collector_service.proto

COPY user-manager/ .

CMD ["python", "app.py"]
