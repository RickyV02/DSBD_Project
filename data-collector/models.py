from database import db
from datetime import datetime, timezone

class UserInterest(db.Model):
    __tablename__ = 'user_interests'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_email = db.Column(db.String(255), nullable=False)
    airport_icao = db.Column(db.String(10), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))

    __table_args__ = (db.UniqueConstraint('user_email', 'airport_icao', name='unique_user_interest'),)

    def to_dict(self):
        return {
            'id': self.id,
            'user_email': self.user_email,
            'airport_icao': self.airport_icao,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

class FlightData(db.Model):
    __tablename__ = 'flight_data'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    airport_icao = db.Column(db.String(10), nullable=False, index=True)
    icao24 = db.Column(db.String(50))
    first_seen = db.Column(db.BigInteger)
    est_departure_airport = db.Column(db.String(10))
    last_seen = db.Column(db.BigInteger)
    est_arrival_airport = db.Column(db.String(10))
    callsign = db.Column(db.String(20))
    est_departure_airport_horiz_distance = db.Column(db.Integer)
    est_departure_airport_vert_distance = db.Column(db.Integer)
    est_arrival_airport_horiz_distance = db.Column(db.Integer)
    est_arrival_airport_vert_distance = db.Column(db.Integer)
    departure_airport_candidates_count = db.Column(db.Integer)
    arrival_airport_candidates_count = db.Column(db.Integer)
    flight_type = db.Column(db.String(20))
    collected_at = db.Column(db.DateTime, default=datetime.now(timezone.utc), index=True)

    __table_args__ = (db.UniqueConstraint('icao24', 'first_seen', 'airport_icao', name='unique_flight'),)

    def to_dict(self):
        return {
            'id': self.id,
            'airport_icao': self.airport_icao,
            'icao24': self.icao24,
            'firstSeen': self.first_seen,
            'estDepartureAirport': self.est_departure_airport,
            'lastSeen': self.last_seen,
            'estArrivalAirport': self.est_arrival_airport,
            'callsign': self.callsign,
            'estDepartureAirportHorizDistance': self.est_departure_airport_horiz_distance,
            'estDepartureAirportVertDistance': self.est_departure_airport_vert_distance,
            'estArrivalAirportHorizDistance': self.est_arrival_airport_horiz_distance,
            'estArrivalAirportVertDistance': self.est_arrival_airport_vert_distance,
            'departureAirportCandidatesCount': self.departure_airport_candidates_count,
            'arrivalAirportCandidatesCount': self.arrival_airport_candidates_count,
            'flight_type': self.flight_type,
            'collected_at': self.collected_at.isoformat() if self.collected_at else None
        }
