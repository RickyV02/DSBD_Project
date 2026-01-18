import grpc
import user_service_pb2
import user_service_pb2_grpc
import os
import json

class UserManagerClient:
    def __init__(self):
        self.host = os.getenv('USER_MANAGER_HOST', 'user-manager')
        self.port = os.getenv('USER_MANAGER_GRPC_PORT', '50051')

        service_config = {
            "methodConfig": [
                {
                    "name": [{"service": "user_service.UserService"}],
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

        # Note: In a high-load production environment with multiple server replicas (in this case, user-manager Pods),
        # we would implement Client-Side Load Balancing to avoid gRPC sticky connections (HTTP/2 persistence).
        # This would require:
        # 1. Using the 'dns:///' scheme in the target (e.g., f'dns:///{self.host}:{self.port}') to resolve all Pods IPs (thanks to Kubernetes DNS, but also works on Docker Compose).
        # 2. Adding ('grpc.lb_policy_name', 'round_robin') to the channel options (so that the client can balance requests across multiple server instances, since it creates N persistent connections).
        # For the current scope, relying on the Kubernetes Service (L4 balancing) is sufficient (this applies only at the establishment of the connection), but we report it just for completeness.

        options = [
            ('grpc.service_config', json.dumps(service_config)),
            ('grpc.keepalive_time_ms', 10000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', 1),
            ('grpc.http2.max_pings_without_data', 0),
        ]

        target = f'{self.host}:{self.port}'
        self.channel = grpc.insecure_channel(target, options=options)
        self.stub = user_service_pb2_grpc.UserServiceStub(self.channel)

    def verify_user(self, email):
        try:
            print("Invio richiesta VerifyUser...", flush=True)
            request = user_service_pb2.VerifyUserRequest(email=email)
            response = self.stub.VerifyUser(request)
            return response.exists, response.message
        except grpc.RpcError as e:
            print(f"Errore gRPC critico dopo retry: {e.details()}", flush=True)
            return False, f"Errore di comunicazione: {e.details()}"

    def get_user(self, email):
        try:
            print("Invio richiesta GetUser...", flush=True)
            request = user_service_pb2.GetUserRequest(email=email)
            response = self.stub.GetUser(request)

            if response.exists:
                return {
                    'email': response.email,
                    'nome': response.nome,
                    'cognome': response.cognome,
                    'codice_fiscale': response.codice_fiscale,
                    'iban': response.iban,
                    'exists': True
                }
            else:
                return {'exists': False}
        except grpc.RpcError as e:
            print(f"Errore gRPC nel recupero utente: {e.details()}", flush=True)
            return {'exists': False, 'error': e.details()}

    def close(self):
        self.channel.close()
