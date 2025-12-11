from kafka import KafkaConsumer, KafkaProducer
import json
import os
import time

def main():
    print("Avvio Alert System Service...", flush=True)

    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    topic_in = 'to-alert-system'
    topic_out = 'to-notifier'
    group_id = 'alert-system-group'

    consumer = None
    producer = None

    while not consumer or not producer:
        try:
            print(f"Tentativo connessione a Kafka ({kafka_bootstrap_servers})...", flush=True)

            consumer = KafkaConsumer(
                topic_in, #Automatic subscription to the topic
                bootstrap_servers=kafka_bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='earliest', # Start reading from the beginning if no offset is committed
                enable_auto_commit=False,     # We want manual commit control
                max_poll_records=10,          # Process up to 10 messages at a time,
                session_timeout_ms=30000,   # Consumer session timeout (30 seconds, default value)
                heartbeat_interval_ms=10000  # Heartbeat interval (10 seconds, default value)
            )

            producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=5,
                linger_ms=5, #This will help reduce the number of requests by batching messages for up to 5ms, and since we want low latency because we are sending alerts, we keep it low
                batch_size=16384, #(it's the default value: 16KB)
                retry_backoff_ms=1000 # Wait 1 second before retrying, just to be safe
            )
            print("Connessione Kafka stabilita con successo!", flush=True)

        except Exception as e:
            print(f"Kafka non pronto, riprovo tra 5s... ({e})", flush=True)
            time.sleep(5)

    print(f"In ascolto sul topic '{topic_in}'...", flush=True)

    try:
        for message in consumer:
            try:
                try:
                    data = json.loads(message.value.decode('utf-8'))
                    print("Alert-System: Messaggio ricevuto:", data, flush=True)
                except json.JSONDecodeError:
                    print(f"ERRORE DATI: Messaggio non valido all'offset {message.offset}. Lo salto.", flush=True)
                    consumer.commit() #We commit garbage data to avoid reprocessing
                    continue

                user_email = data.get('user_email')
                airport_icao = data.get('airport_icao')
                flights_count = data.get('flights_count', 0)
                high_value = data.get('high_value')
                low_value = data.get('low_value')

                alert_condition = None
                if high_value is not None and flights_count > high_value:
                    alert_condition = f"Superata soglia ALTA ({high_value}). Voli rilevati: {flights_count}."
                elif low_value is not None and flights_count < low_value:
                    alert_condition = f"Sotto soglia BASSA ({low_value}). Voli rilevati: {flights_count}."

                if alert_condition:
                    notification = {
                        'email': user_email,
                        'subject': f"Alert Voli Aeroporto: {airport_icao}",
                        'body': alert_condition
                    }
                    producer.send(topic_out, notification)
                    print(f"-> ALERT GENERATO per {user_email}: {alert_condition}", flush=True)
                    producer.flush() # Ensure the message is sent before committing

                consumer.commit()
                print(f"Offset {message.offset} committato dopo elaborazione.", flush=True)

            except Exception as e:
                print(f"ERRORE SISTEMA: {e}. Non commito, riproverò al riavvio.", flush=True)

    except KeyboardInterrupt:
        print("\nInterruzione manuale rilevata. Avvio chiusura risorse...", flush=True)

    finally:
        if producer:
            try:
                producer.close()
                print("Producer chiuso correttamente.", flush=True)
            except Exception as e:
                print(f"Errore durante la chiusura del Producer: {e}", flush=True)

        if consumer:
            try:
                consumer.close()
                print("Consumer chiuso correttamente.", flush=True)
            except Exception as e:
                print(f"Errore durante la chiusura del Consumer: {e}", flush=True)

        print("Alert System terminato.", flush=True)

if __name__ == "__main__":
    main()
