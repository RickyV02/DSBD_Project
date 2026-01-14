from flask import Flask, request, jsonify
from flask_cors import CORS
from database import db
from models import UserInterest, FlightData
from grpc_client import UserManagerClient
from opensky_client import OpenSkyClient
from scheduler import DataCollectorScheduler
import os
import signal
import re
import time
from sqlalchemy import func
from datetime import datetime, timedelta, timezone
import threading
import grpc_server
from prometheus_client import make_wsgi_app, Counter, Gauge
from werkzeug.middleware.dispatcher import DispatcherMiddleware

app = Flask(__name__)
CORS(app)

NODE_NAME = os.getenv('K8S_NODE_NAME', 'unknown-node')
SERVICE_NAME = 'data-collector'

# 1. Counter Metric: Number of calls made to OpenSky API
# Labels: service, node, status (attempt, success, failure)
OPENSKY_CALLS_COUNTER = Counter(
    'opensky_api_calls_total',
    'Total number of calls to OpenSky API',
    ['service', 'node', 'status']
)

# 2. Gauge Metric: Time taken to fetch and store flight data during each periodic collection
# Labels: service, node
FLIGHT_PROCESSING_GAUGE = Gauge(
    'flight_data_processing_seconds',
    'Time taken to fetch and store flight data during periodic collection',
    ['service', 'node']
)

# 3. Counter Metric: Total HTTP requests processed by the Flask app
# Labels: method, endpoint, status code, service, node
HTTP_REQUESTS_TOTAL = Counter(
    'http_requests_total',
    'Total number of HTTP requests processed',
    ['method', 'endpoint', 'status', 'service', 'node']
)

# Middleware to expose /metrics endpoint
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
    '/metrics': make_wsgi_app()
})

# Hook to increment HTTP_REQUESTS_TOTAL after each request
@app.after_request
def record_request_data(response):
    if request.path == '/metrics':
        return response
    try:
        HTTP_REQUESTS_TOTAL.labels(
            method=request.method,
            endpoint=request.path,
            status=str(response.status_code),
            service=SERVICE_NAME,
            node=NODE_NAME
        ).inc()
    except Exception as e:
        print(f"[Prometheus] Errore: {e}", flush=True)
    return response

# Configs for Database
app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql+pymysql://{os.getenv('DATA_DB_USER')}:{os.getenv('DATA_DB_PASSWORD')}@{os.getenv('DATA_DB_HOST')}:{os.getenv('DATA_DB_PORT')}/{os.getenv('DATA_DB_NAME')}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_size': 20, #This is the number of connections to keep open inside the connection pool, can be adjusted based on expected load
    'max_overflow': 10, #This is the number of connections that can be created after the pool reached its size limit, can be adjusted based on expected load spikes
    'pool_recycle': 1800, #Recycle connections after 30 minutes to prevent timeout issues
    'pool_pre_ping': True, #Enable connection health checks
    'pool_timeout': 30  #Timeout for getting a connection from the pool
}

db.init_app(app)

def wait_for_db(app):
    print("Verifica connessione al Database...", flush=True)
    with app.app_context():
        while True:
            try:
                with db.engine.connect() as connection:
                    print("Database pronto! Connessione stabilita.", flush=True)
                    return
            except Exception as e:
                print(f"Database non pronto ({str(e)}). Riprovo tra 3 secondi...", flush=True)
                time.sleep(3)

def initialize_metrics(app):
    print("[Prometheus] Inizializzazione metriche...", flush=True)
    with app.app_context():
        try:
            FLIGHT_PROCESSING_GAUGE.labels(service=SERVICE_NAME, node=NODE_NAME).set(0)
            print("[Prometheus] Gauge Flight Processing inizializzato a 0.", flush=True)
        except Exception as e:
            print(f"[Prometheus] Errore inizializzazione Gauge: {e}", flush=True)

        statuses = ['attempt', 'success', 'failure']
        try:
            for status in statuses:
                OPENSKY_CALLS_COUNTER.labels(
                    service=SERVICE_NAME,
                    node=NODE_NAME,
                    status=status
                ).inc(0)  # Initialize to 0
            print(f"[Prometheus] Counter OpenSky inizializzato per stati: {statuses}", flush=True)
        except Exception as e:
             print(f"[Prometheus] Errore init Counter OpenSky: {e}", flush=True)

with app.app_context():
    wait_for_db(app)
    db.create_all()
    initialize_metrics(app)

def start_grpc_server():
    print("Avvio thread server gRPC Data Collector...", flush=True)
    grpc_server.serve(app)

grpc_thread = threading.Thread(target=start_grpc_server, daemon=True)
grpc_thread.start()

user_manager_client = UserManagerClient()
opensky_client = OpenSkyClient()

scheduler = DataCollectorScheduler(
    app,
    db,
    opensky_client,
    opensky_counter=OPENSKY_CALLS_COUNTER,
    processing_gauge=FLIGHT_PROCESSING_GAUGE,
    service_name=SERVICE_NAME,
    node_name=NODE_NAME
)

collection_interval = int(os.getenv('COLLECTION_INTERVAL_HOURS', '12'))

def is_valid_email(email):
    email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(email_regex, email) is not None

@app.route('/health', methods=['GET'])
def health_check():
    jobs = scheduler.get_jobs()
    return jsonify({
        "status": "healthy",
        "service": "data-collector",
        "scheduler_active": len(jobs) > 0,
        "scheduled_jobs": len(jobs)
    }), 200

@app.route('/interests', methods=['POST'])
def add_interest():
    try:
        # Check if request is valid JSON
        if not request.is_json:
            return jsonify({"error": "Content-Type must be application/json"}), 415

        data = request.json

        if 'email' not in data or 'airport_icao' not in data:
            return jsonify({"error": "Campi 'email' e 'airport_icao' obbligatori"}), 400

        # Input sanitization to ensure consistency (lowercase email, uppercase ICAO)
        email = str(data['email']).strip().lower()
        airport_icao = str(data['airport_icao']).strip().upper()

        # VALIDATION: ICAO Format (4 alphanumeric chars)
        if len(airport_icao) != 4 or not airport_icao.isalnum():
             return jsonify({"error": "Formato Codice ICAO non valido. Deve essere di 4 caratteri (es. LIRF)"}), 400

        if not is_valid_email(email):
            return jsonify({"error": "Formato email non valido"}), 400

        exists, message = user_manager_client.verify_user(email)

        if not exists:
            return jsonify({
                "error": "Utente non trovato",
                "message": message
            }), 404

        # UX IMPROVEMENT: Check if this airport is currently monitored by anyone
        is_new_airport = db.session.execute(
            db.select(func.count()).select_from(UserInterest).filter_by(airport_icao=airport_icao)
        ).scalar() == 0

        # Check if interest already exists for this user
        existing = db.session.execute(db.select(UserInterest).filter_by(user_email=email, airport_icao=airport_icao)).scalar_one_or_none()
        if existing:
            updated = False

            if 'high_value' in data: # Check if key was sent
                val = data['high_value']
                if val is None:
                    existing.high_value = None # Client sent null -> Remove threshold
                    updated = True
                else:
                    # Client sent a value -> Cast and Validate
                    try:
                        val = int(val)
                        if val < 0: return jsonify({"error": "high_value non può essere negativo"}), 400
                        existing.high_value = val
                        updated = True
                    except ValueError:
                        return jsonify({"error": "high_value deve essere un intero"}), 400

            if 'low_value' in data: # Check if key was sent
                val = data['low_value']
                if val is None:
                    existing.low_value = None # Client sent null -> Remove threshold
                    updated = True
                else:
                    try:
                        val = int(val)
                        if val < 0: return jsonify({"error": "low_value non può essere negativo"}), 400
                        existing.low_value = val
                        updated = True
                    except ValueError:
                        return jsonify({"error": "low_value deve essere un intero"}), 400

            h = existing.high_value
            l = existing.low_value
            if h is not None and l is not None and h <= l:
                 db.session.rollback() # Important: undo changes in memory
                 return jsonify({"error": "high_value deve essere maggiore di low_value"}), 400

            if updated:
                db.session.commit()
                return jsonify({
                    "message": "Interesse aggiornato con successo",
                    "interest": existing.to_dict()
                }), 200
            else:
                return jsonify({
                    "message": "Interesse già presente e nessun nuovo valore fornito",
                    "interest": existing.to_dict(),
                    "already_exists": True
                }), 200

        else:
            # Here we use .get() because if missing, None is the correct default for creation
            high_value = data.get('high_value')
            low_value = data.get('low_value')

            # Validation for new entry
            if high_value is not None:
                try:
                    high_value = int(high_value)
                    if high_value < 0: return jsonify({"error": "high_value non può essere negativo"}), 400
                except ValueError: return jsonify({"error": "high_value deve essere un intero"}), 400

            if low_value is not None:
                try:
                    low_value = int(low_value)
                    if low_value < 0: return jsonify({"error": "low_value non può essere negativo"}), 400
                except ValueError: return jsonify({"error": "low_value deve essere un intero"}), 400

            if high_value is not None and low_value is not None:
                if high_value <= low_value:
                    return jsonify({"error": "high_value deve essere maggiore di low_value"}), 400

            interest = UserInterest(user_email=email, airport_icao=airport_icao, high_value=high_value, low_value=low_value)
            db.session.add(interest)
            db.session.commit()
            interest_dict = interest.to_dict()

            # Trigger immediate collection if it's a new airport
            if is_new_airport:
                print(f"Nuovo aeroporto {airport_icao} inserito! Avvio raccolta dati immediata...", flush=True)
                # Run in a separate thread to avoid blocking the HTTP response
                threading.Thread(target=scheduler.collect_single_airport, args=(airport_icao,)).start()

            return jsonify({
                "message": "Interesse aggiunto con successo",
                "interest": interest_dict,
                "already_exists": False,
                "data_collection_triggered": is_new_airport
            }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/interests/<email>', methods=['GET'])
def get_user_interests(email):
    try:
        # Normalize email input from URL
        clean_email = email.strip().lower()

        if not is_valid_email(clean_email):
            return jsonify({"error": "Formato email non valido"}), 400

        exists, message = user_manager_client.verify_user(clean_email)

        if not exists:
            return jsonify({
                "error": "Utente non trovato",
                "message": message
            }), 404

        interests = db.session.execute(db.select(UserInterest).filter_by(user_email=clean_email).order_by(UserInterest.created_at.desc())).scalars().all()

        return jsonify({
            "email": clean_email,
            "interests": [i.to_dict() for i in interests],
            "count": len(interests)
        }), 200

    except Exception as e:
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/interests', methods=['DELETE'])
def remove_interest():
    try:
        email_raw = request.args.get('email')
        icao_raw = request.args.get('airport_icao')

        if not email_raw or not icao_raw:
            return jsonify({"error": "Campi 'email' e 'airport_icao' obbligatori nell'URL"}), 400

        email = email_raw.strip().lower()
        airport_icao = icao_raw.strip().upper()

        if not is_valid_email(email):
             return jsonify({"error": "Formato email non valido"}), 400

        # VALIDATION: ICAO Format
        if len(airport_icao) != 4 or not airport_icao.isalnum():
             return jsonify({"error": "Formato Codice ICAO non valido (4 caratteri richiesti)"}), 400

        exists, message = user_manager_client.verify_user(email)
        if not exists:
            return jsonify({
                "error": "Utente non trovato",
                "message": message
            }), 404

        interest = db.session.execute(db.select(UserInterest).filter_by(user_email=email, airport_icao=airport_icao)).scalar_one_or_none()

        if not interest:
            return jsonify({"error": "Interesse non trovato"}), 404

        interest_dict = interest.to_dict()
        db.session.delete(interest)
        db.session.commit()

        return jsonify({
            "message": "Interesse rimosso con successo",
            "removed": interest_dict
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/flights/<airport_icao>', methods=['GET'])
def get_flights(airport_icao):
    try:
        email = request.args.get('email')

        if not email:
            return jsonify({"error": "Parametro 'email' obbligatorio"}), 400

        clean_email = email.strip().lower()
        clean_icao = airport_icao.strip().upper()

        # VALIDATION: ICAO Format
        if len(clean_icao) != 4 or not clean_icao.isalnum():
             return jsonify({"error": "Formato Codice ICAO non valido (4 caratteri richiesti)"}), 400

        if not is_valid_email(clean_email):
             return jsonify({"error": "Formato email non valido"}), 400

        exists, message = user_manager_client.verify_user(clean_email)
        if not exists:
            return jsonify({
                "error": "Utente non trovato",
                "message": message
            }), 404

        interest = db.session.execute(db.select(UserInterest).filter_by(user_email=clean_email, airport_icao=clean_icao)).scalar_one_or_none()

        if not interest:
            return jsonify({
                "error": "Aeroporto non tra gli interessi dell'utente"
            }), 403

        flight_type = request.args.get('type') # Could be None, or 'departure' or 'arrival'
        start_date_str = request.args.get('start_date') # YYYY-MM-DD
        end_date_str = request.args.get('end_date') # YYYY-MM-DD
        limit = int(request.args.get('limit', 100)) # default to 100

        if flight_type and flight_type not in ['departure', 'arrival']:
            return jsonify({"error": "Il parametro type, se presente, deve essere 'departure' o 'arrival'"}), 400

        # Fix: Enforce a hard limit to prevent DoS attacks
        if limit > 1000:
            limit = 1000

        query = db.select(FlightData).filter_by(airport_icao=clean_icao)

        if flight_type:
            query = query.filter_by(flight_type=flight_type)

        start_date = None
        end_date = None

        if start_date_str:
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                query = query.filter(FlightData.collected_at >= start_date)
            except ValueError:
                return jsonify({"error": "Formato start_date non valido. Usa YYYY-MM-DD"}), 400

        if end_date_str:
            try:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
                end_date_inclusive = end_date.replace(hour=23, minute=59, second=59)
                query = query.filter(FlightData.collected_at <= end_date_inclusive)
            except ValueError:
                return jsonify({"error": "Formato end_date non valido. Usa YYYY-MM-DD"}), 400

        if start_date and end_date and start_date > end_date:
             return jsonify({"error": "La data di inizio non può essere successiva alla data di fine"}), 400

        flights = db.session.execute(query.order_by(FlightData.collected_at.desc()).limit(limit)).scalars().all()

        return jsonify({
            "airport_icao": clean_icao,
            "flights": [f.to_dict() for f in flights],
            "count": len(flights),
            "filters": {
                "type": flight_type,
                "start_date": start_date_str,
                "end_date": end_date_str
            }
        }), 200

    except Exception as e:
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/flights/<airport_icao>/latest', methods=['GET'])
def get_latest_flight(airport_icao):
    try:
        email = request.args.get('email')
        flight_type = request.args.get('type') # None, 'departure', 'arrival'

        if not email:
            return jsonify({"error": "Parametro 'email' obbligatorio"}), 400

        if flight_type and flight_type not in ['departure', 'arrival']:
            return jsonify({"error": "Il parametro type, se presente, deve essere 'departure' o 'arrival'"}), 400

        clean_email = email.strip().lower()
        clean_icao = airport_icao.strip().upper()

        # VALIDATION: ICAO Format
        if len(clean_icao) != 4 or not clean_icao.isalnum():
             return jsonify({"error": "Formato Codice ICAO non valido (4 caratteri richiesti)"}), 400

        if not is_valid_email(clean_email):
             return jsonify({"error": "Formato email non valido"}), 400

        exists, message = user_manager_client.verify_user(clean_email)
        if not exists:
            return jsonify({
                "error": "Utente non trovato",
                "message": message
            }), 404

        interest = db.session.execute(db.select(UserInterest).filter_by(user_email=clean_email, airport_icao=clean_icao)).scalar_one_or_none()
        if not interest:
            return jsonify({"error": "Aeroporto non tra gli interessi dell'utente"}), 403

        def fetch_latest(f_type):
            return db.session.execute(db.select(FlightData).filter_by(
                airport_icao=clean_icao,
                flight_type=f_type
            ).order_by(FlightData.collected_at.desc())).scalars().first()

        response_data = {
            "airport_icao": clean_icao
        }

        if flight_type:
            # User wants only one type (departure OR arrival)
            flight = fetch_latest(flight_type)
            if not flight:
                return jsonify({"message": f"Nessun volo di tipo {flight_type} trovato"}), 404

            response_data['type'] = flight_type
            response_data['flight'] = flight.to_dict()

        else:
            # User wants both types
            last_dep = fetch_latest('departure')
            last_arr = fetch_latest('arrival')

            if not last_dep and not last_arr:
                return jsonify({"message": "Nessun volo trovato"}), 404

            response_data['latest_departure'] = last_dep.to_dict() if last_dep else None
            response_data['latest_arrival'] = last_arr.to_dict() if last_arr else None

        return jsonify(response_data), 200

    except Exception as e:
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/flights/<airport_icao>/average', methods=['GET'])
def get_average_flights(airport_icao):
    try:
        email = request.args.get('email')
        days = int(request.args.get('days', 7))
        # Ensure days is positive
        if days < 1: days = 1

        flight_type = request.args.get('type')

        if not email:
            return jsonify({"error": "Parametro 'email' obbligatorio"}), 400

        if flight_type and flight_type not in ['departure', 'arrival']:
            return jsonify({"error": "Il parametro type, se presente, deve essere 'departure' o 'arrival'"}), 400

        clean_email = email.strip().lower()
        clean_icao = airport_icao.strip().upper()

        # VALIDATION: ICAO Format
        if len(clean_icao) != 4 or not clean_icao.isalnum():
             return jsonify({"error": "Formato Codice ICAO non valido (4 caratteri richiesti)"}), 400

        if not is_valid_email(clean_email):
             return jsonify({"error": "Formato email non valido"}), 400

        exists, message = user_manager_client.verify_user(clean_email)
        if not exists:
            return jsonify({
                "error": "Utente non trovato",
                "message": message
            }), 404

        interest = db.session.execute(db.select(UserInterest).filter_by(user_email=clean_email, airport_icao=clean_icao)).scalar_one_or_none()
        if not interest:
            return jsonify({"error": "Aeroporto non tra gli interessi dell'utente"}), 403

        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

        def calculate_stats(f_type):
            count = db.session.execute(db.select(func.count()).select_from(FlightData).filter(
                FlightData.airport_icao == clean_icao,
                FlightData.flight_type == f_type,
                FlightData.collected_at >= cutoff_date
            )).scalar()
            avg = count / days if days > 0 else 0
            return count, round(avg, 2)

        response_data = {
            "airport_icao": clean_icao,
            "days_analyzed": days
        }

        if flight_type:
            total, avg = calculate_stats(flight_type)
            response_data[flight_type] = {
                "total_flights": total,
                "daily_average": avg
            }
        else:
            tot_dep, avg_dep = calculate_stats('departure')
            tot_arr, avg_arr = calculate_stats('arrival')

            response_data['departure'] = {
                "total_flights": tot_dep,
                "daily_average": avg_dep
            }
            response_data['arrival'] = {
                "total_flights": tot_arr,
                "daily_average": avg_arr
            }

        return jsonify(response_data), 200

    except Exception as e:
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/flights/<airport_icao>/stats/airlines', methods=['GET'])
def get_airline_stats(airport_icao):
    try:
        email = request.args.get('email')
        if not email:
            return jsonify({"error": "Parametro 'email' obbligatorio"}), 400

        clean_email = email.strip().lower()
        clean_icao = airport_icao.strip().upper()

        if len(clean_icao) != 4 or not clean_icao.isalnum():
             return jsonify({"error": "Formato Codice ICAO non valido (4 caratteri richiesti)"}), 400

        if not is_valid_email(clean_email):
             return jsonify({"error": "Formato email non valido"}), 400

        exists, message = user_manager_client.verify_user(clean_email)
        if not exists:
            return jsonify({
                "error": "Utente non trovato",
                "message": message
            }), 404

        interest = db.session.execute(db.select(UserInterest).filter_by(user_email=clean_email, airport_icao=clean_icao)).scalar_one_or_none()
        if not interest:
            return jsonify({"error": "Aeroporto non tra gli interessi dell'utente"}), 403

        # We get the top 5 airlines by number of recorded flights (airlines means unique callsign prefixes, like 'AAL' for American Airlines)

        airline_code = func.substr(FlightData.callsign, 1, 3)

        query = db.session.query(
            airline_code.label('airline'),
            func.count().label('flight_count')
        ).filter(
            FlightData.airport_icao == clean_icao,
            FlightData.callsign != None,
            FlightData.callsign != ''
        ).group_by(
            airline_code
        ).order_by(
            func.count().desc()
        ).limit(5)

        results = query.all()

        stats = [
            {
                "airline_code": row.airline,
                "flights_recorded": row.flight_count
            }
            for row in results
        ]

        return jsonify({
            "airport_icao": clean_icao,
            "stat_type": "top_5_airlines",
            "data": stats
        }), 200

    except Exception as e:
        return jsonify({"error": f"Errore nel calcolo statistiche: {str(e)}"}), 500

@app.route('/collect/manual', methods=['POST'])
def manual_collection():
    try:
        threading.Thread(target=scheduler.collect_data_job).start() # Run in separate thread (performance improvement, we don't block the HTTP response)
        return jsonify({"message": "Raccolta dati avviata manualmente"}), 200
    except Exception as e:
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/scheduler/status', methods=['GET'])
def scheduler_status():
    try:
        jobs = scheduler.get_jobs()
        return jsonify({
            "active": len(jobs) > 0,
            "jobs": [{"id": j.id, "name": j.name, "next_run": str(j.next_run_time)} for j in jobs]
        }), 200
    except Exception as e:
        return jsonify({"error": f"Errore: {str(e)}"}), 500

if __name__ == '__main__':

    def handle_sigterm(*args):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handle_sigterm)
    signal.signal(signal.SIGTERM, handle_sigterm)

    print("Avvio Data Collector Service...", flush=True)
    print(f"REST API sulla porta 5001", flush=True)
    print(f"gRPC Server sulla porta 50052", flush=True)
    print(f"Raccolta dati ogni {collection_interval} ore", flush=True)

    scheduler.start(interval_hours=collection_interval)

    try:
        app.run(host='0.0.0.0', port=5001, debug=False)
    except KeyboardInterrupt:
        print("\nRicevuto segnale di stop. Avvio chiusura...", flush=True)
    finally:
        print("Fermando lo scheduler e il client gRPC...", flush=True)
        scheduler.stop()
        user_manager_client.close()
        print("Data Collector chiuso correttamente.", flush=True)
