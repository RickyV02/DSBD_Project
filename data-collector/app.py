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
        # Fix: Check if request is valid JSON
        if not request.is_json:
            return jsonify({"error": "Content-Type must be application/json"}), 415

        data = request.json

        if 'email' not in data or 'airport_icao' not in data:
            return jsonify({"error": "Campi 'email' e 'airport_icao' obbligatori"}), 400

        # Fix: Input sanitization to ensure consistency (lowercase email, uppercase ICAO)
        email = str(data['email']).strip().lower()
        airport_icao = str(data['airport_icao']).strip().upper()

        exists, message = user_manager_client.verify_user(email)

        if not exists:
            return jsonify({
                "error": "Utente non trovato",
                "message": message
            }), 404

        existing = db.session.execute(db.select(UserInterest).filter_by(user_email=email, airport_icao=airport_icao)).scalar_one_or_none()
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
        db.session.rollback()
        return jsonify({"error": f"Errore: {str(e)}"}), 500

@app.route('/interests/<email>', methods=['GET'])
def get_user_interests(email):
    try:
        # Fix: Normalize email input from URL
        clean_email = email.strip().lower()

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

        # Fix: Normalize inputs
        email = email_raw.strip().lower()
        airport_icao = icao_raw.strip().upper()

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

        # Fix: Normalize email and ICAO
        clean_email = email.strip().lower()
        clean_icao = airport_icao.strip().upper()

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

        flight_type = request.args.get('type') # Could be None, or 'departure' or 'arrival' (even a string not valid, like "foo")

        if flight_type and flight_type not in ['departure', 'arrival']:
            return jsonify({"error": "Il parametro type, se presente, deve essere 'departure' o 'arrival'"}), 400

        limit = int(request.args.get('limit', 100)) # if not provided, default to 100

        # Fix: Enforce a hard limit to prevent DoS attacks (Database overload)
        if limit > 1000:
            limit = 1000

        query = db.select(FlightData).filter_by(airport_icao=clean_icao)

        if flight_type:
            query = query.filter_by(flight_type=flight_type)

        flights = db.session.execute(query.order_by(FlightData.collected_at.desc()).limit(limit)).scalars().all()

        return jsonify({
            "airport_icao": clean_icao,
            "flights": [f.to_dict() for f in flights],
            "count": len(flights)
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

        # Fix: Normalize inputs
        clean_email = email.strip().lower()
        clean_icao = airport_icao.strip().upper()

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
        # Fix: Ensure days is positive
        if days < 1: days = 1

        flight_type = request.args.get('type')

        if not email:
            return jsonify({"error": "Parametro 'email' obbligatorio"}), 400

        if flight_type and flight_type not in ['departure', 'arrival']:
            return jsonify({"error": "Il parametro type, se presente, deve essere 'departure' o 'arrival'"}), 400

        # Fix: Normalize inputs
        clean_email = email.strip().lower()
        clean_icao = airport_icao.strip().upper()

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

    scheduler.start(interval_hours=collection_interval) # Start the scheduler, it will run in background (threaded)

    try:
        app.run(host='0.0.0.0', port=5001, debug=False)
    finally:
        scheduler.stop()
        user_manager_client.close()
