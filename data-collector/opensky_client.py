import requests
import os
import threading
import time
from datetime import datetime, timedelta
from circuit_breaker import CircuitBreaker, CircuitBreakerOpenException

class OpenSkyClient:
    def __init__(self):
        self.api_url = "https://opensky-network.org/api"
        self.auth_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')

        self.token = None
        self.token_expiry = 0

        # Thread safety lock for authentication to prevent race conditions
        self._auth_lock = threading.Lock()

        self.cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)

    def _perform_login(self):
        print(f"[Thread {threading.current_thread().name}] Richiesta nuovo Token di accesso a OpenSky...", flush=True)
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        try:
            response = self.cb.call(requests.post, self.auth_url, data=payload, timeout=10)

            if response.status_code == 200:
                data = response.json()
                self.token = data.get('access_token')
                expires_in = data.get('expires_in', 1800)
                self.token_expiry = time.time() + expires_in - 120  # Refresh 2 minutes before expiry
                print(f"Token ottenuto con successo! Scade tra {expires_in}s.", flush=True)
            else:
                print(f"Errore Autenticazione: {response.status_code} - {response.text}", flush=True)
                self.token = None
        except CircuitBreakerOpenException:
            print("CircuitBreaker OPEN: Impossibile effettuare login.", flush=True)
            self.token = None
        except Exception as e:
            print(f"Errore connessione Auth: {e}", flush=True)

    def _is_token_expired(self):
        return self.token is None or time.time() > self.token_expiry

    def get_headers(self):
        # First check (non-blocking read)
        if self._is_token_expired():
            print(f"[Thread {threading.current_thread().name}] Token è scaduto/mancante. Provo a rinnovare...", flush=True)

            with self._auth_lock:
                # Double Check: Verify again in case another thread refreshed it while we waited
                if self._is_token_expired():
                    self._perform_login()
                else:
                    print(f"[Thread {threading.current_thread().name}] Token già rinnovato da un altro thread! Procedo.", flush=True)

        return {'Authorization': f'Bearer {self.token}'}

    def get_departures(self, airport_icao, begin_timestamp=None, end_timestamp=None):
        if not begin_timestamp:
            begin_timestamp = int((datetime.now() - timedelta(hours=12)).timestamp())
        if not end_timestamp:
            end_timestamp = int(datetime.now().timestamp())

        url = f"{self.api_url}/flights/departure"
        params = {'airport': airport_icao, 'begin': begin_timestamp, 'end': end_timestamp}

        try:
            print(f"Recupero partenze da {airport_icao}...", flush=True)
            # We call get_headers() which handles token refresh automatically
            response = self.cb.call(requests.get, url, params=params, headers=self.get_headers(), timeout=30)

            if response.status_code == 200:
                try:
                    flights = response.json()
                    return flights if flights else []
                except ValueError:
                    print(f"Warning: JSON vuoto/invalido per {airport_icao}", flush=True)
                    return []

            elif response.status_code == 401:
                # 401 Explicitly means Token Expired or Invalid.
                print(f"401 Unauthorized per {airport_icao}. Token scaduto o non valido. Forzo aggiornamento...", flush=True)

                # Set token to None to ensure _perform_login is called next time
                self.token = None

                # Recursive retry (the next call will trigger get_headers -> login)
                # Note: Recursive call is also protected by CB because it calls get_departures again
                return self.get_departures(airport_icao, begin_timestamp, end_timestamp)

            elif response.status_code == 404:
                print(f"Nessun dato trovato per {airport_icao}", flush=True)
                return []

            elif response.status_code == 429:
                # Rate Limiting handling
                print(f"RATE LIMIT EXCEEDED per {airport_icao}. Attendi un minuto...", flush=True)
                return []

            else:
                print(f"Errore API: {response.status_code}", flush=True)
                print(f"Risposta Server: {response.text}", flush=True)
                return []
        except CircuitBreakerOpenException:
            print(f"CircuitBreaker OPEN: Saltata richiesta per {airport_icao}", flush=True)
            return []
        except Exception as e:
            print(f"Errore richiesta: {str(e)}", flush=True)
            return []

    def get_arrivals(self, airport_icao, begin_timestamp=None, end_timestamp=None):
        if not begin_timestamp:
            begin_timestamp = int((datetime.now() - timedelta(hours=12)).timestamp())
        if not end_timestamp:
            end_timestamp = int(datetime.now().timestamp())

        url = f"{self.api_url}/flights/arrival"
        params = {'airport': airport_icao, 'begin': begin_timestamp, 'end': end_timestamp}

        try:
            print(f"Recupero arrivi a {airport_icao}...", flush=True)
            response = self.cb.call(requests.get, url, params=params, headers=self.get_headers(), timeout=30)

            if response.status_code == 200:
                try:
                    flights = response.json()
                    return flights if flights else []
                except ValueError:
                    print(f"Warning: JSON vuoto/invalido per {airport_icao}", flush=True)
                    return []

            elif response.status_code == 401:
                print(f"401 Unauthorized per {airport_icao}. Token scaduto. Forzo aggiornamento...", flush=True)
                self.token = None
                return self.get_arrivals(airport_icao, begin_timestamp, end_timestamp)

            elif response.status_code == 404:
                return []

            elif response.status_code == 429:
                print(f"RATE LIMIT EXCEEDED per {airport_icao}. Attendi un minuto...", flush=True)
                return []

            else:
                print(f"Errore API: {response.status_code}", flush=True)
                print(f"Risposta Server: {response.text}", flush=True)
                return []
        except CircuitBreakerOpenException:
            print(f"CircuitBreaker OPEN: Saltata richiesta per {airport_icao}", flush=True)
            return []
        except Exception as e:
            print(f"Errore richiesta: {str(e)}", flush=True)
            return []

    def get_flights_for_airport(self, airport_icao, begin_timestamp=None, end_timestamp=None):

        departures = self.get_departures(airport_icao, begin_timestamp, end_timestamp)

        time.sleep(0.5)

        arrivals = self.get_arrivals(airport_icao, begin_timestamp, end_timestamp)

        return {
            'departures': departures,
            'arrivals': arrivals,
            'total': len(departures) + len(arrivals)
        }
