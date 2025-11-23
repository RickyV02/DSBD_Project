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
        data = request.json

        required_fields = ['email', 'nome', 'cognome', 'codice_fiscale', 'iban']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Campo '{field}' obbligatorio"}), 400

        request_id = request.headers.get('X-Request-ID') or data.get('request_id') # We can get request_id from header or body, depending on client implementation
        if not request_id:
            unique_string = f"{data['email']}-{data['codice_fiscale']}"
            request_id = hashlib.md5(unique_string.encode()).hexdigest() # If not provided, we generate a deterministic request_id based on data we know are unique

        # Duplicate request check
        query = db.select(User).where(User.request_id == request_id)
        existing_user = db.session.execute(query).scalars().first()
        if existing_user:
            return jsonify({
                "message": "Richiesta già processata!",
                "user": existing_user.to_dict(),
                "idempotent": True
            }), 200

        new_user = User(
            email=data['email'],
            nome=data['nome'],
            cognome=data['cognome'],
            codice_fiscale=data['codice_fiscale'],
            iban=data.get('iban'),
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
        if 'PRIMARY' in str(e) or 'email' in str(e):
            return jsonify({"error": "Email già registrata"}), 409
        elif 'codice_fiscale' in str(e):
            return jsonify({"error": "Codice fiscale già registrato"}), 409
        return jsonify({"error": str(e)}), 409
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Errore durante la registrazione: {str(e)}"}), 500

@app.route('/users/<email>', methods=['GET'])
def get_user(email):
    try:
        user = db.session.get(User, email)

        if not user:
            return jsonify({"error": "Utente non trovato"}), 404

        return jsonify({"user": user.to_dict()}), 200

    except Exception as e:
        return jsonify({"error": f"Errore durante il recupero: {str(e)}"}), 500

@app.route('/users/<email>', methods=['DELETE'])
def delete_user(email):
    try:
        user = db.session.get(User, email)
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
        user = db.session.get(User, email)
        return jsonify({
            "email": email,
            "exists": user is not None
        }), 200

    except Exception as e:
        return jsonify({"error": f"Errore durante la verifica: {str(e)}"}), 500

if __name__ == '__main__':
    print("Avvio User Manager Service...")
    print("REST API sulla porta 5000")
    print("gRPC Server sulla porta 50051")
    app.run(host='0.0.0.0', port=5000, debug=False)
