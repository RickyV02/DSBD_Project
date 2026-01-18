from concurrent import futures
import grpc
import data_collector_service_pb2
import data_collector_service_pb2_grpc
from database import db
from models import UserInterest

class DataCollectorServicer(data_collector_service_pb2_grpc.DataCollectorServiceServicer):
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

                return data_collector_service_pb2.DeleteInterestsResponse(
                    success=True,
                    message=msg
                )
            except Exception as e:
                db.session.rollback()
                error_msg = f"Errore durante la cancellazione interessi: {str(e)}"
                print(error_msg, flush=True)
                return data_collector_service_pb2.DeleteInterestsResponse(
                    success=False,
                    message=error_msg
                )

def serve(app):
    server_options = [
        ('grpc.keepalive_time_ms', 10000),
        ('grpc.keepalive_timeout_ms', 5000),
        ('grpc.http2.min_ping_interval_without_data_ms', 5000),
        ('grpc.http2.max_pings_without_data', 0),
        ('grpc.keepalive_permit_without_calls', 1),
    ]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=server_options)

    data_collector_service_pb2_grpc.add_DataCollectorServiceServicer_to_server(
        DataCollectorServicer(app), server
    )

    server.add_insecure_port('[::]:50052')
    server.start()
    print("Data Collector gRPC Server in ascolto sulla porta 50052...", flush=True)
    server.wait_for_termination()
    return server
