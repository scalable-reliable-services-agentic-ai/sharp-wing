from sqlalchemy import Column, BigInteger, String, Float, Text, Boolean
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()


class Transaction(Base):
    __tablename__ = "transactions"
    transaction_id = Column(BigInteger, primary_key=True)
    client_id = Column(BigInteger)
    receiver_id = Column(BigInteger)
    timestamp_iso = Column(Text)
    transaction_type = Column(Text)
    channel = Column(Text)
    amount = Column(Float)
    currency = Column(Text)
    location = Column(Text)
    ip_address = Column(Text)
    mac_address = Column(Text)
    fingerprint = Column(Text)
    session_id = Column(Text)
    timestamp_ms = Column(BigInteger, primary_key=True)
    current_state = Column(Text)
    history = Column(JSONB)

    # --- New fields added for Persona and Fraud tracking ---
    client_type = Column(Text)
    is_fraud = Column(Boolean, default=False)
    fraud_reason = Column(Text, nullable=True)