from apscheduler.schedulers.background import BackgroundScheduler
from database import db
from models import UserInterest, FlightData
from opensky_client import OpenSkyClient
from datetime import datetime
from flask import Flask

class DataCollectorScheduler:
    def __init__(self, app: Flask, db, opensky_client: OpenSkyClient):
        self.app = app
        self.db = db
        self.opensky_client = opensky_client
        self.scheduler = BackgroundScheduler()

    def collect_data_job(self):
        with self.app.app_context():
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Avvio raccolta dati periodica...")

            airports_query = db.session.query(UserInterest.airport_icao).distinct().all() #if different users have same airport, only query once (optimization)
            airports = [row[0] for row in airports_query]

            if not airports:
                print("Nessun aeroporto da monitorare")
                return

            print(f"Aeroporti da monitorare: {', '.join(airports)}")

            total_saved = 0
            for airport_icao in airports:
                try:
                    print(f"\nElaborazione {airport_icao}...")

                    flights_data = self.opensky_client.get_flights_for_airport(airport_icao)

                    if flights_data['departures']:
                        saved_departures = self._save_flights(
                            airport_icao,
                            flights_data['departures'],
                            'departure'
                        )
                        print(f"Salvate {saved_departures} partenze per {airport_icao}")
                        total_saved += saved_departures

                    if flights_data['arrivals']:
                        saved_arrivals = self._save_flights(
                            airport_icao,
                            flights_data['arrivals'],
                            'arrival'
                        )
                        print(f"Salvati {saved_arrivals} arrivi per {airport_icao}")
                        total_saved += saved_arrivals

                except Exception as e:
                    print(f"Errore nell'elaborazione di {airport_icao}: {str(e)}")
                    continue

            print(f"\nRaccolta completata! Totale voli salvati: {total_saved}")

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

                    if existing: #UPSERT LOGIC: we update only fields that can change over time
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
                    print(f"Errore nel salvataggio del volo {flight.get('icao24')}: {str(e)}")
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

        self.collect_data_job() # Initial download of the data upon start

        self.scheduler.start()
        print(f"Scheduler avviato: raccolta ogni {interval_hours} ore")

    def stop(self):
        self.scheduler.shutdown()
        print("Scheduler fermato")

    def get_jobs(self):
        return self.scheduler.get_jobs()
