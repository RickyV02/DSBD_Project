from concurrent import futures
import grpc
import user_service_pb2
import user_service_pb2_grpc
from database import db
from models import User

class UserServiceServicer(user_service_pb2_grpc.UserServiceServicer):
    def __init__(self, app):
        self.app = app

    def VerifyUser(self, request, context):
        with self.app.app_context():
            try:
                print("Ricevuta richiesta VerifyUser...", flush=True)
                user = db.session.get(User, request.email)
                exists = user is not None
                return user_service_pb2.VerifyUserResponse(
                    exists=exists,
                    message="Utente trovato" if exists else "Utente non trovato"
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                return user_service_pb2.VerifyUserResponse(exists=False, message=f"Errore: {str(e)}")
            finally:
                db.session.remove()

    def GetUser(self, request, context):
        with self.app.app_context():
            print("Ricevuta richiesta GetUser...", flush=True)
            try:
                user = db.session.get(User, request.email)

                if user:
                    return user_service_pb2.GetUserResponse(
                        email=user.email,
                        nome=user.nome,
                        cognome=user.cognome,
                        codice_fiscale=user.codice_fiscale,
                        exists=True,
                        iban=user.iban
                    )
                else:
                    return user_service_pb2.GetUserResponse(
                        email="",
                        nome="",
                        cognome="",
                        codice_fiscale="",
                        exists=False,
                        iban=""
                    )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                return user_service_pb2.GetUserResponse(exists=False)
            finally:
                db.session.remove()

def serve(app):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_service_pb2_grpc.add_UserServiceServicer_to_server(
        UserServiceServicer(app), server
    )
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server gRPC avviato sulla porta 50051", flush=True)
    print("In attesa di richieste...", flush=True)

    server.wait_for_termination()

    return server
