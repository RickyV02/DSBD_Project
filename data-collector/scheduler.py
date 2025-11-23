from apscheduler.schedulers.background import BackgroundScheduler
from database import db
from models import UserInterest, FlightData
from opensky_client import OpenSkyClient
from datetime import datetime
from flask import Flask
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class DataCollectorScheduler:
    def __init__(self, app: Flask, db, opensky_client: OpenSkyClient):
        self.app = app
        self.db = db
        self.opensky_client = opensky_client
        self.scheduler = BackgroundScheduler()

    def collect_data_job(self):
        with self.app.app_context():
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Avvio raccolta dati periodica...")

            airports_query = db.session.query(UserInterest.airport_icao).distinct().all()
            airports = [row[0] for row in airports_query]

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

            #Optimization: 5 threads to fetch multiple airports in parallel (instead of doing one by one sequentially with a for loop)
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

                        print(f"-> Completato {airport_icao}: Salvati {c_dep} partenze, {c_arr} arrivi.")
                        total_saved += (c_dep + c_arr)

                    except Exception as e:
                        print(f"Errore critico salvataggio DB per {airport_icao}: {e}")

            print(f"\nRaccolta completata! Totale operazioni (Insert/Update): {total_saved}")

    def _save_flights(self, airport_icao, flight_data_list, flight_type):
        saved_count = 0
        updated_count = 0

        for flight in flight_data_list:
            try:
                existing = FlightData.query.filter_by(
                    icao24=flight.get('icao24'),
                    first_seen=flight.get('firstSeen'),
                    airport_icao=airport_icao
                ).first()

                if existing: #UPSERT LOGIC: we update existing records instead of creating duplicates
                    existing.last_seen = flight.get('lastSeen')
                    existing.est_arrival_airport = flight.get('estArrivalAirport')
                    existing.callsign = flight.get('callsign')
                    existing.est_arrival_airport_horiz_distance = flight.get('estArrivalAirportHorizDistance')
                    existing.est_arrival_airport_vert_distance = flight.get('estArrivalAirportVertDistance')
                    existing.arrival_airport_candidates_count = flight.get('arrivalAirportCandidatesCount')
                    updated_count += 1
                else:
                    new_flight = FlightData(
                        airport_icao=airport_icao,
                        icao24=flight.get('icao24'),
                        first_seen=flight.get('firstSeen'),
                        est_departure_airport=flight.get('estDepartureAirport'),
                        last_seen=flight.get('lastSeen'),
                        est_arrival_airport=flight.get('estArrivalAirport'),
                        callsign=flight.get('callsign'),
                        est_departure_airport_horiz_distance=flight.get('estDepartureAirportHorizDistance'),
                        est_departure_airport_vert_distance=flight.get('estDepartureAirportVertDistance'),
                        est_arrival_airport_horiz_distance=flight.get('estArrivalAirportHorizDistance'),
                        est_arrival_airport_vert_distance=flight.get('estArrivalAirportVertDistance'),
                        departure_airport_candidates_count=flight.get('departureAirportCandidatesCount'),
                        arrival_airport_candidates_count=flight.get('arrivalAirportCandidatesCount'),
                        flight_type=flight_type
                    )
                    db.session.add(new_flight)
                    saved_count += 1

            except Exception as e:
                print(f"Errore elaborazione volo {flight.get('icao24')}: {str(e)}")
                continue

        if saved_count > 0 or updated_count > 0:
            db.session.commit()

        return saved_count + updated_count

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
