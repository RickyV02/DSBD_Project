from concurrent import futures
import grpc
import user_service_pb2
import user_service_pb2_grpc
from database import db
from models import UserInterest

class DataCollectorServicer(user_service_pb2_grpc.DataCollectorServiceServicer):
    def __init__(self, app):
        self.app = app

    def DeleteInterests(self, request, context):
        with self.app.app_context():
            try:
                print(f"Ricevuta richiesta gRPC di cancellazione interessi per: {request.email}", flush=True)

                deleted = db.session.execute(
                    db.delete(UserInterest).where(UserInterest.user_email == request.email)
                )
                db.session.commit()

                count = deleted.rowcount
                msg = f"Cancellati {count} interessi per {request.email}."
                print(msg, flush=True)

                return user_service_pb2.DeleteInterestsResponse(
                    success=True,
                    message=msg
                )
            except Exception as e:
                db.session.rollback()
                error_msg = f"Errore durante la cancellazione interessi: {str(e)}"
                print(error_msg, flush=True)
                return user_service_pb2.DeleteInterestsResponse(
                    success=False,
                    message=error_msg
                )

def serve(app):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    user_service_pb2_grpc.add_DataCollectorServiceServicer_to_server(
        DataCollectorServicer(app), server
    )

    server.add_insecure_port('[::]:50052')
    server.start()
    print("Data Collector gRPC Server in ascolto sulla porta 50052...", flush=True)
    server.wait_for_termination()
    return server
