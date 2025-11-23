from database import db
from cryptography.fernet import Fernet
import os
import hashlib
from datetime import datetime, timezone

class User(db.Model):
    __tablename__ = 'users'

    email = db.Column(db.String(255), primary_key=True)
    nome = db.Column(db.String(100), nullable=False)
    cognome = db.Column(db.String(100), nullable=False)
    codice_fiscale = db.Column(db.String(16), unique=True, nullable=False)
    _iban = db.Column('iban', db.String(500), nullable=True) #Encrypted IBAN
    iban_hash = db.Column(db.String(64), unique=True, nullable=True) #SHA-256 hash of IBAN for duplicate checking

    data_registrazione = db.Column(db.DateTime, default=datetime.now(timezone.utc))
    request_id = db.Column(db.String(255), unique=True)

    @property
    def iban(self):
        if self._iban:
            try:
                key = os.getenv('ENCRYPTION_KEY').encode()
                f = Fernet(key)
                return f.decrypt(self._iban.encode()).decode()
            except Exception as e:
                print(f"Error decrypting IBAN: {e}")
                return None
        return None

    @iban.setter
    def iban(self, value):
        if value:
            try:
                self.iban_hash = hashlib.sha256(value.encode()).hexdigest() #Unique hash for duplicate checking

                key = os.getenv('ENCRYPTION_KEY').encode()
                f = Fernet(key)
                self._iban = f.encrypt(value.encode()).decode()
            except Exception as e:
                print(f"Error encrypting IBAN: {e}")
                raise e
        else:
            self._iban = None
            self.iban_hash = None

    def to_dict(self):
        return {
            'email': self.email,
            'nome': self.nome,
            'cognome': self.cognome,
            'codice_fiscale': self.codice_fiscale,
            'iban': self.iban, #Decrypted
            'data_registrazione': self.data_registrazione.isoformat() if self.data_registrazione else None,
            'request_id': self.request_id
        }
