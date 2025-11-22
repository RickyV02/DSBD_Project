import requests
import os
from datetime import datetime, timedelta

class OpenSkyClient:
    def __init__(self):
        self.api_url = "https://opensky-network.org/api"
        self.auth_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')

        self.token = None
        self.authenticate()

    def authenticate(self):
        print("Richiesta nuovo Token di accesso a OpenSky...")
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        try:
            response = requests.post(self.auth_url, data=payload)
            if response.status_code == 200:
                self.token = response.json().get('access_token')
                print("Token ottenuto con successo!")
            else:
                print(f"Errore Autenticazione: {response.status_code} - {response.text}")
                self.token = None
        except Exception as e:
            print(f"Errore connessione Auth: {e}")

    def get_headers(self):
        if not self.token:
            self.authenticate()
        return {'Authorization': f'Bearer {self.token}'}

    def get_departures(self, airport_icao, begin_timestamp=None, end_timestamp=None):
        if not begin_timestamp:
            begin_timestamp = int((datetime.now() - timedelta(hours=24)).timestamp())
        if not end_timestamp:
            end_timestamp = int(datetime.now().timestamp())

        url = f"{self.api_url}/flights/departure"
        params = {'airport': airport_icao, 'begin': begin_timestamp, 'end': end_timestamp}

        try:
            print(f"Recupero partenze da {airport_icao}...")
            response = requests.get(url, params=params, headers=self.get_headers(), timeout=30)

            if response.status_code == 200:
                flights = response.json()
                return flights
            elif response.status_code == 401:
                print("Token scaduto! Rinnovo e riprovo...")
                self.authenticate()
                return self.get_departures(airport_icao, begin_timestamp, end_timestamp) # Retry after re-authentication
            elif response.status_code == 404:
                print(f"Nessun dato trovato per {airport_icao}")
                return []
            else:
                print(f"Errore API: {response.status_code}")
                return []
        except Exception as e:
            print(f"Errore richiesta: {str(e)}")
            return []

    def get_arrivals(self, airport_icao, begin_timestamp=None, end_timestamp=None):
        if not begin_timestamp:
            begin_timestamp = int((datetime.now() - timedelta(hours=24)).timestamp())
        if not end_timestamp:
            end_timestamp = int(datetime.now().timestamp())

        url = f"{self.api_url}/flights/arrival"
        params = {'airport': airport_icao, 'begin': begin_timestamp, 'end': end_timestamp}

        try:
            print(f"Recupero arrivi a {airport_icao}...")
            response = requests.get(url, params=params, headers=self.get_headers(), timeout=30)

            if response.status_code == 200:
                flights = response.json()
                return flights
            elif response.status_code == 401:
                self.authenticate()
                return self.get_arrivals(airport_icao, begin_timestamp, end_timestamp) # Retry after re-authentication
            elif response.status_code == 404:
                return []
            else:
                print(f"Errore API: {response.status_code}")
                return []
        except Exception as e:
            print(f"Errore richiesta: {str(e)}")
            return []

    def get_flights_for_airport(self, airport_icao, begin_timestamp=None, end_timestamp=None):

        departures = self.get_departures(airport_icao, begin_timestamp, end_timestamp)
        arrivals = self.get_arrivals(airport_icao, begin_timestamp, end_timestamp)

        return {
            'departures': departures,
            'arrivals': arrivals,
            'total': len(departures) + len(arrivals)
        }
