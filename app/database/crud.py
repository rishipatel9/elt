from sqlalchemy.orm import Session
from . import models
from ..schemas import schemas

def bulk_create_transactions(db: Session, transactions: list[schemas.TransactionCreate]):
    try:
        db_transactions = [
            models.MergedTransaction(
                order_id=transaction.order_id,
                transaction_type=transaction.transaction_type,
                payment_type=transaction.payment_type,
                net_amount=transaction.net_amount,
                invoice_amount=transaction.invoice_amount,
            )
            for transaction in transactions
        ]
        db.bulk_save_objects(db_transactions)
        db.commit()
    except Exception as e:
        db.rollback()
        print("Error during bulk insert:", str(e))
        raise

def log_event(db: Session, log: schemas.LogCreate):
    db_log = models.LogEntry(**log.dict())
    db.add(db_log)
    db.commit()
    return db_log
