from apscheduler.schedulers.background import BackgroundScheduler
from database import db
from models import UserInterest, FlightData
from opensky_client import OpenSkyClient
from datetime import datetime
from flask import Flask
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from sqlalchemy import select, delete, not_
from sqlalchemy.dialects.mysql import insert # Importing the specific MySQL dialect 'insert' to enable the ON DUPLICATE KEY UPDATE' feature (Upsert).
from kafka import KafkaProducer
import json
import os
import time
import random

class DataCollectorScheduler:
    def __init__(self, app: Flask, db, opensky_client: OpenSkyClient):
        self.app = app
        self.db = db
        self.opensky_client = opensky_client
        self.scheduler = BackgroundScheduler()

        self.kafka_producer = None
        self.topic = 'to-alert-system'

        self.kafka_lock = threading.Lock()

        # CONCURRENCY CONTROL:
        # Set to track airports currently being updated to avoid race conditions
        # between manual triggers and the periodic job.
        self.processing_airports = set()
        self.processing_lock = threading.Lock()

        self._connect_kafka()

    def _connect_kafka(self):
        try:
            kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'), # We use json serialization for messages
                acks='all', #Ensure the highest level of message durability by waiting for all replicas to acknowledge
                retries=5,
                linger_ms=50, #Reduce latency by batching messages for up to 50ms
                batch_size=32768,     # Increase batch size to 32KB for better throughput
                retry_backoff_ms=1000 # Wait 1 second before retrying, just to be safe
            )
            print(f"Kafka Producer connesso a {kafka_bootstrap_servers}", flush=True)
        except Exception as e:
            print(f"Errore connessione Kafka: {e}", flush=True)
            self.kafka_producer = None

    def _acquire_lock(self, icao):
        with self.processing_lock:
            if icao in self.processing_airports:
                return False
            self.processing_airports.add(icao)
            return True

    def _release_lock(self, icao):
        with self.processing_lock:
            self.processing_airports.discard(icao)

    def collect_single_airport(self, target_icao):
        thread_name = threading.current_thread().name

        if not self._acquire_lock(target_icao):
            print(f"[{thread_name}] SKIP {target_icao}: Già in fase di aggiornamento.", flush=True)
            return

        try:
            if not self.kafka_producer:
                with self.kafka_lock:
                    if not self.kafka_producer:
                        self._connect_kafka()

            with self.app.app_context():
                try:
                    print(f"\n[{thread_name}] [Trigger] Avvio raccolta mirata per {target_icao}...", flush=True)

                    query = select(UserInterest).filter_by(airport_icao=target_icao)
                    interests_objs = db.session.execute(query).scalars().all()

                    if not interests_objs:
                        print(f"[{thread_name}] Nessun interesse trovato per {target_icao}, abort.", flush=True)
                        return

                    try:
                        flight_data = self.opensky_client.get_flights_for_airport(target_icao)
                    except Exception as e:
                        print(f"[{thread_name}] Errore download {target_icao}: {e}", flush=True)
                        return

                    if not flight_data:
                        return

                    interests = [i.to_dict() for i in interests_objs]

                    self._process_airport_data(target_icao, flight_data, interests)

                finally:
                    db.session.remove()

        except Exception as e:
            print(f"[{thread_name}] Errore durante la raccolta mirata: {e}", flush=True)
        finally:
            self._release_lock(target_icao)

    def collect_data_job(self):
        if not self.kafka_producer:
            print("Kafka Producer non connesso, tentando riconnessione...", flush=True)
            with self.kafka_lock:
                if not self.kafka_producer:
                    self._connect_kafka()

        with self.app.app_context():
            try:
                print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Avvio raccolta dati periodica...", flush=True)

                # Fetch ALL active interests
                query = select(UserInterest)
                interests = db.session.execute(query).scalars().all()

                airport_interests = {}
                for interest in interests:
                    if interest.airport_icao not in airport_interests:
                        airport_interests[interest.airport_icao] = []
                    airport_interests[interest.airport_icao].append(interest.to_dict())

                active_airports = list(airport_interests.keys())

                # Garbage Collection
                try:
                    if not active_airports:
                        print("Nessun interesse attivo. Pulizia completa voli...", flush=True)
                        deleted = db.session.execute(delete(FlightData))
                        db.session.commit()
                        if deleted.rowcount > 0:
                            print(f"Pulizia completata: rimossi {deleted.rowcount} voli.", flush=True)
                        return
                    else:
                        # Delete flight data where airport_icao is NOT IN active_airports
                        print("Pulizia voli di aeroporti non più monitorati...", flush=True)
                        cleanup_query = delete(FlightData).where(
                            not_(FlightData.airport_icao.in_(active_airports))
                        )
                        deleted = db.session.execute(cleanup_query)
                        db.session.commit()
                        if deleted.rowcount > 0:
                            print(f"Pulizia completata: rimossi {deleted.rowcount} voli.", flush=True)
                        else:
                            print("Nessun volo da pulire.", flush=True)
                except Exception as e:
                    db.session.rollback()
                    print(f"Errore durante la pulizia voli: {e}", flush=True)

                print(f"Aeroporti da monitorare: {', '.join(active_airports)}", flush=True)

                # Using a wrapper to handle locking per airport task
                def process_wrapper(icao):
                    with self.app.app_context():
                        thread_name = threading.current_thread().name
                        if not self._acquire_lock(icao):
                            print(f"[{thread_name}] SKIP {icao}: Già in aggiornamento da altro thread.", flush=True)
                            return

                        try:
                            print(f"[{thread_name}] Richiedo dati per {icao} a OpenSky...", flush=True)
                            data = self.opensky_client.get_flights_for_airport(icao)
                            if data:
                                print(f"[{thread_name}] Dati scaricati per {icao}, elaborazione...", flush=True)
                                self._process_airport_data(icao, data, airport_interests[icao])
                                print(f"[{thread_name}] Elaborazione completata per {icao}.", flush=True)
                        except Exception as e:
                            print(f"[{thread_name}] Errore processamento {icao}: {e}", flush=True)
                        finally:
                            self._release_lock(icao)
                            db.session.remove()

                # Execute tasks in parallel
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = [executor.submit(process_wrapper, icao) for icao in active_airports]
                    for _ in as_completed(futures):
                        pass

                print(f"\nRaccolta periodica completata.", flush=True)

            except Exception as e:
                print(f"Errore generale nel job di raccolta dati: {e}", flush=True)

            finally:
                db.session.remove()

    def _process_airport_data(self, airport_icao, flight_data, interests):
        c_dep = 0
        c_arr = 0

        if not flight_data or (not flight_data.get('departures') and not flight_data.get('arrivals')):
            print(f"[{airport_icao}] Nessun dato voli da salvare.", flush=True)
            return

        db_success = False

        try:
            if flight_data.get('departures'):
                c_dep = self._save_flights(airport_icao, flight_data['departures'], 'departure')

            if flight_data.get('arrivals'):
                c_arr = self._save_flights(airport_icao, flight_data['arrivals'], 'arrival')

            print(f"-> DB OK {airport_icao}: Salvati {c_dep} partenze, {c_arr} arrivi.", flush=True)
            db_success = True

        except Exception as e:
            db.session.rollback()
            print(f"Errore critico salvataggio DB per {airport_icao}: {e}", flush=True)
            db_success = False

        if db_success:
            messages_to_send = []
            try:
                for interest in interests:
                    messages_to_send.append({
                        'user_email': interest['user_email'],
                        'airport_icao': airport_icao,
                        'flights_count': c_dep + c_arr,
                        'departures_count': c_dep,
                        'arrivals_count': c_arr,
                        'high_value': interest['high_value'],
                        'low_value': interest['low_value']
                    })
            except Exception as e:
                print(f"Errore lettura dati per invio Kafka {airport_icao}: {e}", flush=True)
                return

            max_kafka_retries = 5

            for attempt in range(max_kafka_retries):
                if not self.kafka_producer:
                    with self.kafka_lock:
                        if not self.kafka_producer:
                            self._connect_kafka()

                if self.kafka_producer:
                    try:
                        sent_count = 0
                        for msg in messages_to_send:
                            self.kafka_producer.send(self.topic, msg)
                            sent_count += 1

                        self.kafka_producer.flush()
                        print(f"-> Kafka OK: Inviati {sent_count} alert per {airport_icao}.", flush=True)
                        break

                    except Exception as k_err:
                        print(f"ERRORE Kafka per {airport_icao} (Tentativo {attempt+1}/{max_kafka_retries}): {k_err}", flush=True)
                        try:
                            self.kafka_producer.close()
                        except:
                            pass
                        self.kafka_producer = None

                        if attempt < max_kafka_retries - 1:
                            print("Attendo 2 secondi prima di riprovare...", flush=True)
                            time.sleep(2)
                        else:
                            print(f"-> Kafka ERROR: Impossibile inviare alert per {airport_icao} dopo {max_kafka_retries} tentativi.", flush=True)
                else:
                    if attempt < max_kafka_retries - 1:
                        print(f"Kafka non disponibile. Riprovo tra 2s...", flush=True)
                        time.sleep(2)
                    else:
                        print(f"-> Kafka ERROR: Impossibile connettersi. Alert saltati per {airport_icao}.", flush=True)

    def _save_flights(self, airport_icao, flight_data_list, flight_type):
        if not flight_data_list:
            return 0

        # PERFORMANCE IMPROVEMENT:
        # Instead of executing a DB query for every single flight inside the loop (O(N) complexity),
        # we now simply prepare a list of dictionaries in memory (O(1) complexity).
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

        max_retries = 5
        for attempt in range(max_retries):
            try:
                db.session.execute(on_duplicate_key_query)
                db.session.commit()
                return len(insert_values)

            except Exception as e:
                db.session.rollback()
                error_str = str(e).lower()
                if "deadlock" in error_str or "1213" in error_str:
                    if attempt < max_retries - 1:
                        sleep_time = random.uniform(0.5, 2.0) #Basically, to prevent deadlocks, we wait a random time before retrying (like exponential backoff)
                        print(f"Deadlock rilevato per {airport_icao}. Riprovo ({attempt+1}/{max_retries})...", flush=True)
                        time.sleep(sleep_time)
                        continue

                print(f"Errore bulk upsert per {airport_icao}: {e}", flush=True)
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
        print(f"Scheduler avviato: raccolta ogni {interval_hours} ore", flush=True)

    def stop(self):
        self.scheduler.shutdown()
        print("Scheduler fermato", flush=True)
        if self.kafka_producer:
            try:
                self.kafka_producer.close()
                print("Producer Kafka chiuso correttamente.", flush=True)
            except Exception as e:
                print(f"Errore durante la chiusura del Producer: {e}", flush=True)

    def get_jobs(self):
        return self.scheduler.get_jobs()
