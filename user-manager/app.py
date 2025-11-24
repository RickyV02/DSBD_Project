from flask import Flask, request, jsonify
from flask_cors import CORS
from database import db
from models import User
import grpc_server
import threading
import os
import hashlib
from sqlalchemy.exc import IntegrityError

app = Flask(__name__)
CORS(app)

# Configs for Database
app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False # to suppress warnings (disables signaling feature, avoiding overhead)

db.init_app(app)

with app.app_context():
    db.create_all()

# In order to handle both REST and gRPC servers, we start the gRPC server in a separate thread !
def start_grpc_server():
    grpc_server.serve(app)

grpc_thread = threading.Thread(target=start_grpc_server, daemon=True) # Daemon thread will die when main program exits !
grpc_thread.start()

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "user-manager"}), 200

@app.route('/users', methods=['POST'])
def register_user(): #We register a new user with at-most-once policy. The client can send a request_id to ensure idempotency.
    try:
        # Fix: Check if request is valid JSON to avoid crashes
        if not request.is_json:
            return jsonify({"error": "Content-Type must be application/json"}), 415

        data = request.json

        # Fix: Sanitizing inputs to handle case sensitivity and extra spaces
        email = str(data.get('email', '')).strip().lower()
        nome = str(data.get('nome', '')).strip()
        cognome = str(data.get('cognome', '')).strip()
        codice_fiscale = str(data.get('codice_fiscale', '')).strip().upper()
        iban = str(data.get('iban', '')).strip().upper()

        # Fix: Validating sanitized fields are not empty (instead of checking raw 'data')
        if not email or not nome or not cognome or not codice_fiscale:
             return jsonify({"error": "Campi obbligatori mancanti o vuoti"}), 400

        # Fix: Robust Request ID extraction. Using 'is not None' to handle '0' (integer) correctly.
        req_id_input = request.headers.get('X-Request-ID') or data.get('request_id')

        if req_id_input is not None:
             request_id = str(req_id_input) # We can get request_id from header or body, depending on client implementation
        else:
            iban_value = iban # use sanitized iban
            unique_string = f"{email}-{codice_fiscale}-{iban_value}"
            request_id = hashlib.md5(unique_string.encode()).hexdigest() # Generate a simple hash as request_id if not provided, using email, codice_fiscale and iban (which are unique per user)

        # Duplicate request check
        query = db.select(User).where(User.request_id == request_id)
        existing_user = db.session.execute(query).scalar_one_or_none()

        if existing_user:
            #We check if ALL the unique fields are already registered but we received different nome/cognome, we have to block this request
            # Fix: compare with sanitized variables
            is_mismatch = (existing_user.nome != nome or existing_user.cognome != cognome)

            if is_mismatch:
                return jsonify({
                    "message": "Attenzione: Utente già esistente con dati chiave identici.",
                    "idempotent": True,
                    "existing_user": existing_user.to_dict() # Just to check the real user with that unique data
                }), 409

            # if all the data are equal, that's a real idempotency request
            else:
                return jsonify({
                    "message": "Richiesta già processata!",
                    "user": existing_user.to_dict(),
                    "idempotent": True
                }), 200

        new_user = User(
            email=email,
            nome=nome,
            cognome=cognome,
            codice_fiscale=codice_fiscale,
            iban=iban,
            request_id=request_id
        )

        db.session.add(new_user)
        db.session.commit()

        return jsonify({
            "message": "Utente registrato con successo",
            "user": new_user.to_dict(),
            "request_id": request_id
        }), 201

    except IntegrityError as e:
        db.session.rollback()
        error_msg = str(e.orig)

        print(f"IntegrityError DB: {error_msg}")

        if 'codice_fiscale' in error_msg:
            return jsonify({"error": "Codice fiscale già registrato"}), 409

        elif 'iban_hash' in error_msg:
             return jsonify({"error": "IBAN già registrato"}), 409

        elif 'PRIMARY' in error_msg or 'email' in error_msg or 'request_id' in error_msg:
            return jsonify({"error": "Email o Request ID già registrata"}), 409

        return jsonify({"error": f"Dati duplicati: {error_msg}"}), 409

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Errore durante la registrazione: {str(e)}"}), 500

@app.route('/users/<email>', methods=['GET'])
def get_user(email):
    try:
        # Fix: Normalize email before querying (strip spaces and lowercase)
        clean_email = email.strip().lower()
        user = db.session.get(User, clean_email)

        if not user:
            return jsonify({"error": "Utente non trovato"}), 404

        return jsonify({"user": user.to_dict()}), 200

    except Exception as e:
        return jsonify({"error": f"Errore durante il recupero: {str(e)}"}), 500

@app.route('/users/<email>', methods=['DELETE'])
def delete_user(email):
    try:
        # Fix: Normalize email here too for consistency
        clean_email = email.strip().lower()
        user = db.session.get(User, clean_email)
        if not user:
            return jsonify({"error": "Utente non trovato"}), 404

        user_dict = user.to_dict()
        db.session.delete(user)
        db.session.commit()

        return jsonify({
            "message": "Utente eliminato con successo",
            "user": user_dict
        }), 200

    except Exception as e:
        db.session.rollback()
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
        # Fix: Normalize email for verification
        clean_email = email.strip().lower()
        user = db.session.get(User, clean_email)
        return jsonify({
            "email": clean_email,
            "exists": user is not None
        }), 200

    except Exception as e:
        return jsonify({"error": f"Errore durante la verifica: {str(e)}"}), 500

if __name__ == '__main__':
    print("Avvio User Manager Service...")
    print("REST API sulla porta 5000")
    print("gRPC Server sulla porta 50051")
    app.run(host='0.0.0.0', port=5000, debug=False)
