from apscheduler.schedulers.background import BackgroundScheduler
from database import db
from models import UserInterest, FlightData
from opensky_client import OpenSkyClient
from datetime import datetime
from flask import Flask
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert # Importing the specific MySQL dialect 'insert' to enable the ON DUPLICATE KEY UPDATE' feature (Upsert).

class DataCollectorScheduler:
    def __init__(self, app: Flask, db, opensky_client: OpenSkyClient):
        self.app = app
        self.db = db
        self.opensky_client = opensky_client
        self.scheduler = BackgroundScheduler()

    def collect_data_job(self):
        with self.app.app_context():
            try:
                print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Avvio raccolta dati periodica...")

                query = select(UserInterest.airport_icao).distinct()
                airports_query = db.session.execute(query).scalars().all()
                airports = list(airports_query)

                if not airports:
                    print("Nessun aeroporto da monitorare")
                    return

                print(f"Aeroporti da monitorare: {', '.join(airports)}")
                total_saved = 0

                def fetch_airport_data(icao):
                    thread_name = threading.current_thread().name
                    print(f"Thread {thread_name} avviato per: {icao}")
                    try:
                        print(f"Richiesta OpenSky per {icao} in corso...")
                        data = self.opensky_client.get_flights_for_airport(icao)
                        return icao, data
                    except Exception as e:
                        print(f"Errore download {icao}: {e}")
                        return icao, None

                with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_icao = {executor.submit(fetch_airport_data, icao): icao for icao in airports}

                    for future in as_completed(future_to_icao):
                        airport_icao, flight_data = future.result()

                        if not flight_data:
                            continue

                        try:
                            c_dep = 0
                            c_arr = 0

                            if flight_data.get('departures'):
                                c_dep = self._save_flights(airport_icao, flight_data['departures'], 'departure')

                            if flight_data.get('arrivals'):
                                c_arr = self._save_flights(airport_icao, flight_data['arrivals'], 'arrival')

                            print(f"-> Completato {airport_icao}: Processati {c_dep} partenze, {c_arr} arrivi.")
                            total_saved += (c_dep + c_arr)

                        except Exception as e:
                            db.session.rollback()
                            print(f"Errore critico salvataggio DB per {airport_icao}: {e}")

                print(f"\nRaccolta completata! Totale voli processati: {total_saved}")

            except Exception as e:
                print(f"Errore generale nel job di raccolta dati: {e}")

            finally:
                db.session.remove()

    def _save_flights(self, airport_icao, flight_data_list, flight_type):
        if not flight_data_list:
            return 0

        # PERFORMANCE IMPROVEMENT:
        # Instead of executing a DB query for every single flight inside the loop (N+1 complexity),
        # we now simply prepare a list of dictionaries in memory.
        # This avoids network latency and massive DB I/O overhead.
        insert_values = []
        for flight in flight_data_list:
            if not flight.get('icao24') or not flight.get('firstSeen'):
                continue

            insert_values.append({
                "airport_icao": airport_icao,
                "icao24": flight.get('icao24'),
                "first_seen": flight.get('firstSeen'),
                "est_departure_airport": flight.get('estDepartureAirport'),
                "last_seen": flight.get('lastSeen'),
                "est_arrival_airport": flight.get('estArrivalAirport'),
                "callsign": flight.get('callsign'),
                "est_departure_airport_horiz_distance": flight.get('estDepartureAirportHorizDistance'),
                "est_departure_airport_vert_distance": flight.get('estDepartureAirportVertDistance'),
                "est_arrival_airport_horiz_distance": flight.get('estArrivalAirportHorizDistance'),
                "est_arrival_airport_vert_distance": flight.get('estArrivalAirportVertDistance'),
                "departure_airport_candidates_count": flight.get('departureAirportCandidatesCount'),
                "arrival_airport_candidates_count": flight.get('arrivalAirportCandidatesCount'),
                "flight_type": flight_type,
                "collected_at": datetime.now()
            })

        if not insert_values:
            return 0

        query = insert(FlightData).values(insert_values)

        # "ON DUPLICATE KEY UPDATE" clause:
        # This logic delegates the "check if exists" to the Database Engine.
        # If the Unique Constraint (icao24 + first_seen + airport_icao) matches a row,
        # it updates the specified fields. If not, it inserts a new row.
        on_duplicate_key_query = query.on_duplicate_key_update(
            last_seen=query.inserted.last_seen,
            est_arrival_airport=query.inserted.est_arrival_airport,
            callsign=query.inserted.callsign,
            est_arrival_airport_horiz_distance=query.inserted.est_arrival_airport_horiz_distance,
            est_arrival_airport_vert_distance=query.inserted.est_arrival_airport_vert_distance,
            arrival_airport_candidates_count=query.inserted.arrival_airport_candidates_count,
            collected_at=datetime.now() # Update timestamp to know we saw this flight again
        )

        try:
            # ATOMIC EXECUTION:
            # Execute the single massive query. This replaces hundreds of individual
            # SELECT/INSERT/UPDATE queries, significantly reducing execution time.
            db.session.execute(on_duplicate_key_query)
            db.session.commit()
            return len(insert_values)

        except Exception as e:
            db.session.rollback()
            print(f"Errore bulk upsert per {airport_icao}: {e}")
            raise e

    def start(self, interval_hours=12):
        self.scheduler.add_job(
            self.collect_data_job,
            'interval',
            hours=interval_hours,
            id='collect_flight_data',
            name='Raccolta Dati Voli',
            replace_existing=True
        )
        self.collect_data_job()
        self.scheduler.start()
        print(f"Scheduler avviato: raccolta ogni {interval_hours} ore")

    def stop(self):
        self.scheduler.shutdown()
        print("Scheduler fermato")

    def get_jobs(self):
        return self.scheduler.get_jobs()
