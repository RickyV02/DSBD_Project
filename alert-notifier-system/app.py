from kafka import KafkaConsumer
import json
import os
import time
import smtplib
import ssl
from email.message import EmailMessage
import signal
from prometheus_client import start_http_server, Counter, Gauge

NODE_NAME = os.getenv('K8S_NODE_NAME', 'unknown-node')
SERVICE_NAME = 'alert-notifier-system'

EMAILS_SENT_TOTAL = Counter(
    'emails_sent_total',
    'Total number of alert emails sent',
    ['service', 'node', 'status']
)

LAST_EMAIL_DURATION = Gauge(
    'last_email_sent_duration_seconds',
    'Time spent sending the last alert email in seconds',
    ['service', 'node']
)

SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 465))
SMTP_USER = os.getenv('SMTP_USER', 'your_alert_sender@example.com')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', 'your_app_password_here')
SENDER_EMAIL = os.getenv('SENDER_EMAIL', 'your_alert_sender@example.com')


def send_email(recipient_email, subject, body):
    start_time = time.time()

    context = ssl.create_default_context()

    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = recipient_email

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)

    duration = time.time() - start_time

    LAST_EMAIL_DURATION.labels(
        service=SERVICE_NAME,
        node=NODE_NAME
    ).set(duration)

    print(f"EMAIL INVIATA con successo a: {recipient_email}")
    return True

def initialize_metrics():
    print("[Prometheus] Inizializzazione metriche a 0...", flush=True)
    try:
        EMAILS_SENT_TOTAL.labels(service=SERVICE_NAME, node=NODE_NAME, status='success').inc(0)
        EMAILS_SENT_TOTAL.labels(service=SERVICE_NAME, node=NODE_NAME, status='failure').inc(0)

        LAST_EMAIL_DURATION.labels(service=SERVICE_NAME, node=NODE_NAME).set(0)

        print("[Prometheus] Metriche inizializzate.", flush=True)
    except Exception as e:
        print(f"[Prometheus] Errore inizializzazione metriche: {e}", flush=True)

def main():

    start_http_server(8000)

    print("Avvio Alert Notifier System...", flush=True)

    initialize_metrics()

    def handle_sigterm(*args):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handle_sigterm)
    signal.signal(signal.SIGTERM, handle_sigterm)

    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    topic_in = 'to-notifier'
    group_id = 'alert-notifier-group'

    consumer = None

    while consumer is None:
        try:
            print(f"Tentativo connessione a Kafka ({kafka_bootstrap_servers})...", flush=True)

            #This consumer, since uses SMTP, which could lead to delays, needs "special" custom config in order TO NOT BE KICKED OUT OF THE GROUP
            consumer = KafkaConsumer(
                topic_in,
                bootstrap_servers=kafka_bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                max_poll_records=10, #Process up to 10 messages at a time (so we have time to send emails without being kicked out)
                session_timeout_ms=30000, #Consumer session timeout (30 seconds)
                heartbeat_interval_ms=10000 #Heartbeat interval (10 seconds) ---> as written in Kafka docs, should be about a third of session_timeout_ms
            )
            print("Kafka Consumer connesso.", flush=True)

        except (Exception) as e:
            print(f"Kafka non pronto, riprovo tra 5s... ({e})", flush=True)
            time.sleep(5)

    print("In attesa di notifiche...", flush=True)

    try:
        for message in consumer:
            try:
                try:
                    data = json.loads(message.value.decode('utf-8'))
                    print("Alert-notifier: Messaggio ricevuto dal topic.", flush=True)
                except json.JSONDecodeError:
                    print(f"ERRORE DATI: Messaggio non valido all'offset {message.offset}. Lo salto.", flush=True)
                    consumer.commit()
                    continue

                email = data.get('email')
                subject = data.get('subject')
                body = data.get('body')

                print(f"Tentativo di invio email alert a {email}...", flush=True)
                send_email(email, subject, body)

                EMAILS_SENT_TOTAL.labels(
                    service=SERVICE_NAME,
                    node=NODE_NAME,
                    status='success'
                ).inc()

                consumer.commit()
                print(f"Offset {message.offset} committato dopo invio email.", flush=True)

            except smtplib.SMTPException as e:
                EMAILS_SENT_TOTAL.labels(
                    service=SERVICE_NAME,
                    node=NODE_NAME,
                    status='failure'
                ).inc()
                print(f"ERRORE SISTEMA (SMTP): Invio email fallito. ({e}). Non committo l'offset.", flush=True)
            except Exception as e:
                EMAILS_SENT_TOTAL.labels(
                    service=SERVICE_NAME,
                    node=NODE_NAME,
                    status='failure'
                ).inc()
                print(f"ERRORE GENERALE: {e}. Non committo l'offset.", flush=True)


    except KeyboardInterrupt:
        print("\nInterruzione manuale rilevata. Avvio chiusura risorse...", flush=True)

    finally:
        if consumer:
            try:
                consumer.close()
                print("Consumer chiuso correttamente.", flush=True)
            except Exception as e:
                print(f"Errore durante la chiusura del Consumer: {e}", flush=True)

        print("Alert Notifier System terminato.", flush=True)

if __name__ == "__main__":
    main()
