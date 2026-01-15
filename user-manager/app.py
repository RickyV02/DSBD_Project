from flask import Flask, request, jsonify
from flask_cors import CORS
from database import db
from models import User, RequestCache
import grpc_server
import threading
import os
import signal
import hashlib
import re
import uuid
import json
import time
from datetime import datetime, timedelta, timezone
from sqlalchemy.exc import IntegrityError, OperationalError
from grpc_client import DataCollectorClient
from prometheus_client import make_wsgi_app, Counter, Gauge # make_wsgi_app to expose /metrics endpoint (it's a WSGI app, so like Flask but only for metrics)
from werkzeug.middleware.dispatcher import DispatcherMiddleware # To combine Flask app with Prometheus WSGI app (we use a middleware to serve both apps on different paths)

app = Flask(__name__)
CORS(app)

# Environment Variables for Node and Service Identification
# (to get the real node nome and not the pod name, which can be ephemeral, we'll use K8S_NODE_NAME set by the downward API in the deployment manifest)
NODE_NAME = os.getenv('K8S_NODE_NAME', 'unknown-node')
SERVICE_NAME = 'user-manager'

# 1. Counter Metric: These count occurrences of events.
# Total number of HTTP requests processed, labeled by method, endpoint, status code, service, and node.
HTTP_REQUESTS_TOTAL = Counter(
    'http_requests_total',
    'Total number of HTTP requests processed',
    ['method', 'endpoint', 'status', 'service', 'node']
)

# 2. Gauge Metric: These represent values that can go up and down.
# Total number of registered users in the database, labeled by service and node.
ACTIVE_USERS_GAUGE = Gauge(
    'active_users_total',
    'Total number of registered users in the db (synced every 10 seconds)',
    ['service', 'node']
)

# Cache Metrics: Track cache cleanup duration (Gauge) and number of cleaned entries (Counter)
CACHE_CLEANUP_DURATION = Gauge(
    'cache_cleanup_duration_seconds',
    'Time spent cleaning up expired cache entries during the last run',
    ['service', 'node']
)

CACHE_CLEANED_ENTRIES = Counter(
    'cache_cleaned_entries_total',
    'Total number of expired cache entries removed',
    ['service', 'node']
)

# That's it! We don't need to register metrics manually in each endpoint.
# We'll use a hook to automatically increment the HTTP_REQUESTS_TOTAL counter after each request (since we want to track all endpoints and status codes, not only successful ones).
# And this middleware will expose the /metrics endpoint for Prometheus to scrape.
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
    '/metrics': make_wsgi_app()
})

@app.after_request
def record_request_data(response): # This function is called after each request to record metrics.
    # Avoid counting the /metrics endpoint itself to prevent recursion
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
        print(f"[Prometheus] Errore aggiornamento metriche: {e}", flush=True)

    return response

# Configs for Database
app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False # to suppress warnings (disables signaling feature, avoiding overhead)
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_pre_ping': True, # Enable pool pre-ping to avoid stale connections
    'pool_recycle': 3600, # Recycle connections after 1 hour
    'pool_size': 10, # Set the pool size
    'pool_timeout': 30,  #Timeout for getting a connection from the pool
    'max_overflow': 20 # Allow overflow connections
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

def sync_user_metrics():
    print("[Metrics Sync] Avvio thread di sincronizzazione utenti...", flush=True)
    while True:
        with app.app_context():
            try:
                user_count = db.session.query(User).count()
                ACTIVE_USERS_GAUGE.labels(service=SERVICE_NAME, node=NODE_NAME).set(user_count)
            except Exception as e:
                print(f"[Metrics Sync] Errore durante il sync: {e}", flush=True)
        
        time.sleep(10)

def initialize_metrics(app): # We initialize the metrics at startup, so that we can see them immediately in Prometheus.
    print("[Prometheus] Inizializzazione metriche...", flush=True)
    with app.app_context():
        try:
            CACHE_CLEANED_ENTRIES.labels(service=SERVICE_NAME, node=NODE_NAME).inc(0) # Initialize to 0
            print("[Prometheus] Cache Counter inizializzato a 0.", flush=True)
        except Exception as e:
            print(f"[Prometheus] Errore inizializzazione Cache Counter: {e}", flush=True)

        try:
            CACHE_CLEANUP_DURATION.labels(service=SERVICE_NAME, node=NODE_NAME).set(0)
            print("[Prometheus] Cache Cleanup Gauge inizializzato a 0.", flush=True)
        except Exception as e:
            print(f"[Prometheus] Errore inizializzazione Cache Gauge: {e}", flush=True)

with app.app_context():
    wait_for_db(app)
    try:
        db.create_all()
        print("Tabelle del database create correttamente.", flush=True)
    except OperationalError as e: # EDIT: Catching OperationalError to handle "table already exists" case, useful for concurrent startups with multiple replicas.
        orig = getattr(e, "orig", None)
        if orig and getattr(orig, "args", None) and orig.args[0] == 1050: # Error code for "Table already exists"
            print("Le tabelle del database esistono già. Continuo...", flush=True)
        else:
            print(f"Errore durante la creazione delle tabelle: {e}", flush=True)
            raise
    initialize_metrics(app)

# In order to handle both REST and gRPC servers, we start the gRPC server in a separate thread !
def start_grpc_server():
    print("Avvio thread server gRPC User Manager...", flush=True)
    grpc_server.serve(app)

grpc_thread = threading.Thread(target=start_grpc_server, daemon=True)
grpc_thread.start()

metrics_thread = threading.Thread(target=sync_user_metrics, daemon=True)
metrics_thread.start()

data_collector_client = DataCollectorClient()

# Cache Cleaner Thread
def clean_request_cache():
    while True:
        with app.app_context():
            try:
                start_time = time.time()

                # If a request is retried after 5 minutes, it's treated as a new attempt.
                # Cache entries older than 5 minutes are deleted.
                expiration_time = datetime.now(timezone.utc) - timedelta(minutes=5)

                deleted = db.session.execute(
                    db.delete(RequestCache).where(RequestCache.created_at < expiration_time)
                )
                db.session.commit()

                duration = time.time() - start_time
                CACHE_CLEANUP_DURATION.labels(service=SERVICE_NAME, node=NODE_NAME).set(duration)

                if deleted.rowcount > 0:
                    print(f"[Cache Cleaner] Rimossi {deleted.rowcount} elementi in {duration:.4f}s.", flush=True)
                    CACHE_CLEANED_ENTRIES.labels(service=SERVICE_NAME, node=NODE_NAME).inc(deleted.rowcount)
                else:
                    print(f"[Cache Cleaner] No request cache entries to remove.", flush=True)
            except Exception as e:
                print(f"[Cache Cleaner] Error: {e}", flush=True)

        # Run every 5 minutes
        time.sleep(300)

cleaner_thread = threading.Thread(target=clean_request_cache, daemon=True)
cleaner_thread.start()

def is_valid_email(email):
    email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(email_regex, email) is not None

def is_valid_codice_fiscale(cf):
    cf_regex = r'^[A-Z]{6}[0-9]{2}[A-Z][0-9]{2}[A-Z][0-9]{3}[A-Z]$'
    return re.match(cf_regex, cf) is not None

def is_valid_iban(iban):
    iban_regex = r'^IT[0-9]{2}[A-Z][0-9]{10}[0-9A-Z]{12}$'
    return re.match(iban_regex, iban) is not None

# Helper for robust input sanitization: handles None values (JSON null) gracefully
def get_clean_input(data, key):
    val = data.get(key)
    if val is None:
        return ""
    return str(val).strip()

# Helper for UUID validation
def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False

# Helper to identify the client via hashed IP address
def get_client_id():
    ip = request.headers.get('X-Real-IP') # Try to get the real client IP from Nginx header

    if not ip:
        ip = request.remote_addr or "unknown" # Fallback to remote_addr if header not present (that's the case without Nginx, like in Postman tests directly to Flask in the previous version).

    # Basically, if there is not Nginx and we contact Flask via postman, remote_addr is the IP of the client.
    # If there is Nginx, it passes the real client IP in X-Real-IP header (otherwise remote_addr is Nginx's gateway IP, which is useless for us).

    return hashlib.sha256(ip.encode()).hexdigest()

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "user-manager"}), 200

@app.route('/users', methods=['POST'])
def register_user(): #We register a new user with at-most-once policy. The client can send a request_id to ensure idempotency.
    try:
        #Check if request is valid JSON to avoid crashes
        if not request.is_json:
            return jsonify({"error": "Content-Type must be application/json"}), 415

        data = request.json

        # Calculate unique key based on Client ID (hash of IP address) + Request ID
        client_id = get_client_id()
        req_id_input = request.headers.get('X-Request-ID') or data.get('request_id')

        if req_id_input:
            # Client provided a Request ID: must be a valid UUID.
            if not is_valid_uuid(req_id_input):
                return jsonify({"error": "X-Request-ID/header 'request_id' must be a valid UUID"}), 400
            request_id = str(req_id_input)
        else:
            # Fallback: Hash of the FULL FORM data.
            # This logic provides robust idempotency for User Registration because this operation
            # is idempotent by domain definition (creating the exact same user twice is impossible).
            # Therefore, receiving identical data implies an identical intent (retry), eliminating the risk
            # of "collision of intent" that would exist in non-idempotent operations (e.g., money transfers,
            # where same data could mean a second, distinct transaction).

            # Using sort_keys=True ensures determinism (same JSON fields = same hash).
            # We exclude request_id from the hash if it was inside the body but empty
            data_to_hash = {k: v for k, v in data.items() if k != 'request_id'}
            form_string = json.dumps(data_to_hash, sort_keys=True)
            request_id = hashlib.sha256(form_string.encode()).hexdigest()

        # Final Cache Key
        idempotency_key = hashlib.sha256(f"{client_id}:{request_id}".encode()).hexdigest()

        # Check if we already processed this exact request from this client.
        cached_response = db.session.get(RequestCache, idempotency_key)
        if cached_response:
            print(f"Hit Cache: {request_id}", flush=True)
            # Return the CACHED response. This hides the fact that it's a retry.
            return jsonify(json.loads(cached_response.response_body)), cached_response.response_code

        #Sanitizing inputs to handle case sensitivity and extra spaces
        # Using helper to prevent crashes on JSON null values
        email = get_clean_input(data, 'email').lower()
        nome = get_clean_input(data, 'nome')
        cognome = get_clean_input(data, 'cognome')
        codice_fiscale = get_clean_input(data, 'codice_fiscale').upper()
        iban = get_clean_input(data, 'iban').upper()

        if not email or not nome or not cognome or not codice_fiscale or not iban:
            return jsonify({"error": "Campi obbligatori mancanti o vuoti"}), 400

        if not is_valid_email(email):
            return jsonify({"error": "Formato email non valido"}), 400

        if not is_valid_codice_fiscale(codice_fiscale):
            return jsonify({"error": "Formato codice fiscale non valido"}), 400

        if not is_valid_iban(iban):
            return jsonify({"error": "Formato IBAN non valido"}), 400

        new_user = User(
            email=email,
            nome=nome,
            cognome=cognome,
            codice_fiscale=codice_fiscale,
            iban=iban
        )

        db.session.add(new_user)

        db.session.flush()
        db.session.refresh(new_user) #Ensure new_user has up-to-date data (e.g., auto-generated fields), rollback is still possible.

        response_body = {
            "message": "Utente registrato con successo",
            "user": new_user.to_dict(),
            "request_id": request_id
        }
        response_code = 201

        new_cache_entry = RequestCache(
            id=idempotency_key,
            response_body=json.dumps(response_body),
            response_code=response_code
        )
        db.session.add(new_cache_entry)

        db.session.commit()

        return jsonify(response_body), response_code

    except IntegrityError as e:
        db.session.rollback()
        error_msg = str(e.orig)

        print(f"IntegrityError DB: {error_msg}", flush=True)

        if 'codice_fiscale' in error_msg:
            return jsonify({"error": "Codice fiscale già registrato"}), 409

        elif 'iban_hash' in error_msg:
             return jsonify({"error": "IBAN già registrato"}), 409

        elif 'PRIMARY' in error_msg or 'email' in error_msg:
            return jsonify({"error": "Email già registrata"}), 409

        # Rare case: Race condition on Request Cache insertion
        if 'request_cache' in error_msg:
             # If we hit here, another concurrent request from the same client with the same Request ID
             # was processed just before us. This means we can safely return the cached response.
             cached = db.session.get(RequestCache, idempotency_key)
             if cached:
                 return jsonify(json.loads(cached.response_body)), cached.response_code

        return jsonify({"error": f"Dati duplicati: {error_msg}"}), 409

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Errore durante la registrazione: {str(e)}"}), 500

@app.route('/users/<email>', methods=['GET'])
def get_user(email):
    try:
        #Normalize email before querying (strip spaces and lowercase)
        clean_email = email.strip().lower()

        if not is_valid_email(clean_email):
            return jsonify({"error": "Formato email non valido"}), 400

        user = db.session.get(User, clean_email)

        if not user:
            return jsonify({"error": "Utente non trovato"}), 404

        return jsonify({"user": user.to_dict()}), 200

    except Exception as e:
        return jsonify({"error": f"Errore durante il recupero: {str(e)}"}), 500

@app.route('/users/<email>', methods=['DELETE'])
def delete_user(email):
    try:
        clean_email = email.strip().lower()

        if not is_valid_email(clean_email):
            return jsonify({"error": "Formato email non valido"}), 400

        user = db.session.get(User, clean_email)
        if not user:
            return jsonify({"error": "Utente non trovato"}), 404

        user_dict = user.to_dict()

        # PHASE 1: PIVOT TRANSACTION (gRPC Remote Cleanup)
        # We verify if the remote cleanup on Data Collector is successful first.
        # This acts as a "Pre-Commit Check" and our Point of No Return.
        # Note: We haven't touched the local DB session yet to keep it clean.

        grpc_success, grpc_msg = data_collector_client.delete_interests(clean_email) # THIS IS A PIVOT TRANSACTIONAL STEP

        if not grpc_success:
            # The remote dependency failed. Since we haven't modified the local DB yet,
            # we just abort. No rollback needed (session is clean), but we ensure consistency and we do it just for safety.
            db.session.rollback()
            return jsonify({
                "error": "Impossibile completare la cancellazione: Errore di comunicazione con Data Collector.",
                "details": grpc_msg
            }), 503 # Service Unavailable

        # PHASE 2: RETRYABLE TRANSACTION (Local Commit with Hybrid Retry)
        # Remote call was successful. We can now safely commit the local change.
        # Since we passed the Pivot, this step MUST succeed eventually.
        # We implement a Short-Term Retry mechanism here to handle transient DB locks/errors.

        MAX_RETRIES = 3

        for attempt in range(MAX_RETRIES):
            try:
                # We mark the user for deletion.
                # IMPORTANT: We use merge() because if a previous attempt in this loop failed
                # and rolled back, the 'user' object might be detached from the session.
                user_to_delete = db.session.merge(user)
                db.session.delete(user_to_delete)

                # RETRYABLE OPERATION: Attempting to commit local changes.
                db.session.commit()

                # Success!
                return jsonify({
                    "message": "Utente eliminato con successo",
                    "user": user_dict,
                    "data_cleanup": "Completed"
                }), 200

            except Exception as db_err:
                # If local deletion fails, the user remains in DB but with no interests in Data Collector.
                # We rollback this specific attempt.
                db.session.rollback()

                if attempt < MAX_RETRIES - 1:
                    # Short-Term Retry: Wait a bit and try again (Forward Recovery).
                    print(f"Commit Locale fallito (tentativo {attempt+1}/{MAX_RETRIES}). Ritento...", flush=True)
                    time.sleep(0.5)
                    continue
                else:
                    # If we run out of retries, we raise the error.
                    # This triggers the 500 response, delegating the Long-Term Retry to the client.
                    # Thanks to Idempotency on Data Collector, the user can safely retry later.
                    print(f"Errore critico al DB dopo {MAX_RETRIES} tentativi. Delego al client di ritentare la cancellazione.", flush=True)
                    raise db_err

    except Exception as e:
        return jsonify({"error": f"Errore durante l'eliminazione: {str(e)}"}), 500

@app.route('/users', methods=['GET'])
def get_all_users():
    try:
        query = db.select(User)
        users = db.session.execute(query).scalars().all()

        return jsonify({
            "count": len(users),
            "users": [user.to_dict() for user in users]
        }), 200

    except Exception as e:
        return jsonify({"error": f"Errore durante il recupero: {str(e)}"}), 500

@app.route('/users/verify/<email>', methods=['GET'])
def verify_user(email):
    try:
        clean_email = email.strip().lower()

        if not is_valid_email(clean_email):
            return jsonify({"error": "Formato email non valido"}), 400

        user = db.session.get(User, clean_email)
        return jsonify({
            "email": clean_email,
            "exists": user is not None
        }), 200

    except Exception as e:
        return jsonify({"error": f"Errore durante la verifica: {str(e)}"}), 500

if __name__ == '__main__':

    def handle_sigterm(*args):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handle_sigterm)
    signal.signal(signal.SIGTERM, handle_sigterm)

    print("Avvio User Manager Service...", flush=True)
    print("REST API sulla porta 5000", flush=True)
    print("gRPC Server sulla porta 50051", flush=True)

    try:
        app.run(host='0.0.0.0', port=5000, debug=False)
    except KeyboardInterrupt:
        print("\nRicevuto segnale di stop. Avvio chiusura...", flush=True)
    finally:
        print("Chiusura User Manager...", flush=True)
        data_collector_client.close()
        print("User Manager chiuso correttamente.", flush=True)
