import grpc
import user_service_pb2
import user_service_pb2_grpc
import os

class UserManagerClient:
    def __init__(self):
        self.host = os.getenv('USER_MANAGER_HOST', 'user-manager')
        self.port = os.getenv('USER_MANAGER_GRPC_PORT', '50051')
        self.channel = grpc.insecure_channel(f'{self.host}:{self.port}')
        self.stub = user_service_pb2_grpc.UserServiceStub(self.channel)

    def verify_user(self, email):
        try:
            request = user_service_pb2.VerifyUserRequest(email=email)
            response = self.stub.VerifyUser(request)
            return response.exists, response.message
        except grpc.RpcError as e:
            print(f"Errore gRPC nella verifica utente: {e.details()}")
            return False, f"Errore di comunicazione: {e.details()}"

    def get_user(self, email):
        try:
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
            print(f"Errore gRPC nel recupero utente: {e.details()}")
            return {'exists': False, 'error': e.details()}

    def close(self):
        self.channel.close()
