from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime
from .database import Base

class MergedTransaction(Base):
    __tablename__ = "merged_transactions"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, nullable=True, index=True)
    transaction_type = Column(String, nullable=False)
    payment_type = Column(String, nullable=True)
    net_amount = Column(Float, nullable=True)
    invoice_amount = Column(Float, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)

class OrderSummary(Base):
    __tablename__ = "order_summary"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, nullable=False, unique=True)
    category = Column(String, nullable=False)
    net_amount_sum = Column(Float, nullable=True)
    invoice_amount_sum = Column(Float, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)

class LogEntry(Base):
    __tablename__ = "log_entries"
    id = Column(Integer, primary_key=True, index=True)
    level = Column(String, nullable=False)
    message = Column(String, nullable=False)
    details = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
