from sqlalchemy.orm import Session
from . import models
from ..schemas import schemas

def create_transaction(db: Session, transaction: schemas.TransactionCreate):
    db_transaction = models.MergedTransaction(**transaction.dict())
    db.add(db_transaction)
    db.commit()
    db.refresh(db_transaction)
    return db_transaction

def log_event(db: Session, log: schemas.LogCreate):
    db_log = models.LogEntry(**log.dict())
    db.add(db_log)
    db.commit()
    return db_log
