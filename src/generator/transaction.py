from datetime import datetime, timezone
import uuid
import random


class Transaction:
    def __init__(self):
        self.transaction_id = self._create_transaction_id()
        self.sender_id = self._create_client_id()
        self.receiver_id = self._create_client_id()
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.type, self.channel = self._create_pair_type_channel()
        self.amount = self._create_amount()
        self.currency = self._get_currency()
        self.status = "PENDING"
        self.geolocation = f"{random.uniform(-90, 90)}, {random.uniform(-180, 180)}"
        self.ip_address = f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
        self.mac_address = f"{random.randint(0, 255):02x}:{random.randint(0, 255):02x}:{random.randint(0, 255):02x}:{random.randint(0, 255):02x}:{random.randint(0, 255):02x}:{random.randint(0, 255):02x}"
        self.fingerprint = str(uuid.uuid4())
        self.session_id = str(uuid.uuid4())
        self.is_fraud = False
        self.fraud_reason = "None"
        self.client_type = "Unknown"

    def _create_transaction_id(self):
        day_code = datetime.now().strftime("%Y%m%d")
        random_part = str(random.randint(10_000, 99_999))
        return int(day_code + random_part)

    def _create_client_id(self):
        return random.randint(100_000, 999_999)

    def _create_amount(self):
        """Generates a valid transaction amount."""
        return round(random.uniform(1.0, 99999.0), 2)

    def _create_pair_type_channel(self):
        # all channels:
        # ["mobile", "web", "bank", "atm", "pos"]

        type_to_channels = {
            "deposit": ["mobile", "atm", "bank"],
            "withdrawal": ["mobile", "atm", "bank"],
            "transfer": ["mobile", "web", "bank"],
            "payment": ["mobile", "web", "pos"],
        }

        trans_type = random.choice(list(type_to_channels.keys()))
        trans_channel = random.choice(type_to_channels[trans_type])
        return trans_type, trans_channel

    def _get_currency(self):
        currencies = ["USD", "EUR", "GBP", "JPY", "CHF", "PLN"]
        probability = [0.5, 0.3, 0.1, 0.06, 0.035, 0.005]
        return random.choices(currencies, probability, k=1)[0]

    def generate_transaction_data(self):
        """Generates a dictionary of random data for the template."""
        return {
            "transaction_id": self.transaction_id,
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "timestamp": self.timestamp,
            "transaction_type": self.type,
            "channel": self.channel,
            "amount": self.amount,
            "currency": self.currency,
            "status": self.status,
            "geolocation": self.geolocation,
            "ip_address": self.ip_address,
            "mac_address": self.mac_address,
            "fingerprint": self.fingerprint,
            "session_id": self.session_id,
            "is_fraud": self.is_fraud,
            "fraud_reason": self.fraud_reason,
            "client_type": self.client_type
        }
