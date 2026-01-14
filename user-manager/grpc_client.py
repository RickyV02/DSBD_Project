import grpc
import data_collector_service_pb2
import data_collector_service_pb2_grpc
import os
import json

class DataCollectorClient:
    def __init__(self):
        self.host = os.getenv('DATA_COLLECTOR_HOST', 'data-collector')
        self.port = os.getenv('DATA_COLLECTOR_GRPC_PORT', '50052')

        service_config = {
            "methodConfig": [
                {
                    "name": [{"service": "data_collector_service.DataCollectorService"}],
                    "retryPolicy": {
                        "maxAttempts": 5,
                        "initialBackoff": "1s",
                        "maxBackoff": "5s",
                        "backoffMultiplier": 2,
                        "retryableStatusCodes": ["UNAVAILABLE"]
                    },
                    "timeout": "10s"
                }
            ]
        }

        options = [
            ('grpc.service_config', json.dumps(service_config)),
            ('grpc.keepalive_time_ms', 10000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', 1),
            ('grpc.http2.max_pings_without_data', 0),
        ]

        target = f'{self.host}:{self.port}'
        self.channel = grpc.insecure_channel(target, options=options)
        self.stub = data_collector_service_pb2_grpc.DataCollectorServiceStub(self.channel)

    def delete_interests(self, email):
        try:
            print(f"Invio richiesta gRPC cancellazione interessi per {email}...", flush=True)
            request = data_collector_service_pb2.DeleteInterestsRequest(email=email)
            response = self.stub.DeleteInterests(request)
            if response.success:
                print(f"Successo gRPC: {response.message}", flush=True)
            else:
                print(f"Fallimento gRPC lato server: {response.message}", flush=True)
            return response.success, response.message
        except grpc.RpcError as e:
            print(f"Errore critico di connessione gRPC verso Data Collector: {e.details()}", flush=True)
            return False, str(e)

    def close(self):
        self.channel.close()
