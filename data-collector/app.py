from flask import Flask, request, jsonify
from flask_cors import CORS
from database import db
from models import UserInterest, FlightData
from grpc_client import UserManagerClient
from opensky_client import OpenSkyClient
from scheduler import DataCollectorScheduler
import os
from sqlalchemy import func
from datetime import datetime, timedelta, timezone

app = Flask(__name__)
CORS(app)

# Configs for Database
app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql+pymysql://{os.getenv('DATA_DB_USER')}:{os.getenv('DATA_DB_PASSWORD')}@{os.getenv('DATA_DB_HOST')}:{os.getenv('DATA_DB_PORT')}/{os.getenv('DATA_DB_NAME')}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)

with app.app_context():
    db.create_all()

user_manager_client = UserManagerClient()
opensky_client = OpenSkyClient()

scheduler = DataCollectorScheduler(app, db, opensky_client)
collection_interval = int(os.getenv('COLLECTION_INTERVAL_HOURS', '12'))

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
        data = request.json

        if 'email' not in data or 'airport_icao' not in data:
            return jsonify({"error": "Campi 'email' e 'airport_icao' obbligatori"}), 400

        email = data['email']
        airport_icao = data['airport_icao'].upper()

        exists, message = user_manager_client.verify_user(email)

        if not exists:
            return jsonify({
                "error": "Utente non trovato",
                "message": message
            }), 404

        existing = UserInterest.query.filter_by(user_email=email, airport_icao=airport_icao).first()
        if not existing:
            interest = UserInterest(user_email=email, airport_icao=airport_icao)
            db.session.add(interest)
            db.session.commit()
            interest_dict = interest.to_dict()
        else:
            interest_dict = existing.to_dict()

        return jsonify({
            "message": "Interesse aggiunto con successo",
            "interest": interest_dict
        }), 201

    except Exception as e:
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/interests/<email>', methods=['GET'])
def get_user_interests(email):
    try:
        exists, message = user_manager_client.verify_user(email)

        if not exists:
            return jsonify({"error": "Utente non trovato"}), 404

        interests = UserInterest.query.filter_by(user_email=email).order_by(UserInterest.created_at.desc()).all()

        return jsonify({
            "email": email,
            "interests": [i.to_dict() for i in interests],
            "count": len(interests)
        }), 200

    except Exception as e:
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/interests', methods=['DELETE'])
def remove_interest():
    try:
        data = request.json

        if 'email' not in data or 'airport_icao' not in data:
            return jsonify({"error": "Campi 'email' e 'airport_icao' obbligatori"}), 400

        email = data['email']
        airport_icao = data['airport_icao'].upper()

        interest = UserInterest.query.filter_by(user_email=email, airport_icao=airport_icao).first()

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
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/flights/<airport_icao>', methods=['GET'])
def get_flights(airport_icao):
    try:
        email = request.args.get('email')

        if not email:
            return jsonify({"error": "Parametro 'email' obbligatorio"}), 400

        exists, _ = user_manager_client.verify_user(email)
        if not exists:
            return jsonify({"error": "Utente non trovato"}), 404

        interest = UserInterest.query.filter_by(user_email=email, airport_icao=airport_icao.upper()).first()

        if not interest:
            return jsonify({
                "error": "Aeroporto non tra gli interessi dell'utente"
            }), 403

        flight_type = request.args.get('type')  # 'departure', 'arrival', or None
        limit = int(request.args.get('limit', 100))

        query = FlightData.query.filter_by(airport_icao=airport_icao.upper())
        if flight_type:
            query = query.filter_by(flight_type=flight_type)

        flights = query.order_by(FlightData.collected_at.desc()).limit(limit).all()

        return jsonify({
            "airport_icao": airport_icao.upper(),
            "flights": [f.to_dict() for f in flights],
            "count": len(flights)
        }), 200

    except Exception as e:
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/flights/<airport_icao>/latest', methods=['GET'])
def get_latest_flight(airport_icao):
    try:
        email = request.args.get('email')
        flight_type = request.args.get('type', 'departure')  # 'departure' or 'arrival'

        if not email:
            return jsonify({"error": "Parametro 'email' obbligatorio"}), 400

        if flight_type not in ['departure', 'arrival']:
            return jsonify({"error": "Tipo deve essere 'departure' o 'arrival'"}), 400

        exists, _ = user_manager_client.verify_user(email)
        if not exists:
            return jsonify({"error": "Utente non trovato"}), 404

        interest = UserInterest.query.filter_by(user_email=email, airport_icao=airport_icao.upper()).first()

        if not interest:
            return jsonify({"error": "Aeroporto non tra gli interessi dell'utente"}), 403

        flight = FlightData.query.filter_by(
            airport_icao=airport_icao.upper(),
            flight_type=flight_type
        ).order_by(FlightData.collected_at.desc()).first()

        if not flight:
            return jsonify({
                "message": "Nessun volo trovato",
                "airport_icao": airport_icao.upper(),
                "flight_type": flight_type
            }), 404

        return jsonify({
            "airport_icao": airport_icao.upper(),
            "flight_type": flight_type,
            "flight": flight.to_dict()
        }), 200

    except Exception as e:
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/flights/<airport_icao>/average', methods=['GET'])
def get_average_flights(airport_icao):
    try:
        email = request.args.get('email')
        days = int(request.args.get('days', 7))
        flight_type = request.args.get('type', 'departure')  # 'departure' or 'arrival'

        if not email:
            return jsonify({"error": "Parametro 'email' obbligatorio"}), 400

        if flight_type not in ['departure', 'arrival']:
            return jsonify({"error": "Tipo deve essere 'departure' o 'arrival'"}), 400

        exists, _ = user_manager_client.verify_user(email)
        if not exists:
            return jsonify({"error": "Utente non trovato"}), 404

        interest = UserInterest.query.filter_by(user_email=email, airport_icao=airport_icao.upper()).first()

        if not interest:
            return jsonify({"error": "Aeroporto non tra gli interessi dell'utente"}), 403

        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

        total_flights = FlightData.query.filter(
            FlightData.airport_icao == airport_icao.upper(),
            FlightData.flight_type == flight_type,
            FlightData.collected_at >= cutoff_date
        ).count()

        average = total_flights / days if days > 0 else 0

        return jsonify({
            "airport_icao": airport_icao.upper(),
            "flight_type": flight_type,
            "days": days,
            "total_flights": total_flights,
            "daily_average": round(average, 2)
        }), 200

    except Exception as e:
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/collect/manual', methods=['POST'])
def manual_collection():
    try:
        scheduler.collect_data_job()
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
    print("Avvio Data Collector Service...")
    print(f"REST API sulla porta 5001")
    print(f"Raccolta dati ogni {collection_interval} ore")

    scheduler.start(interval_hours=collection_interval)

    try:
        app.run(host='0.0.0.0', port=5001, debug=False)
    finally:
        scheduler.stop()
        user_manager_client.close()
