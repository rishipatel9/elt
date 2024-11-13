from sqlalchemy.orm import Session
from ..database import crud
from ..schemas import schemas

def log_info(db: Session, message: str):
    log = schemas.LogCreate(level="INFO", message=message)
    crud.log_event(db, log)

def log_error(db: Session, message: str, details: str = ""):
    log = schemas.LogCreate(level="ERROR", message=message, details=details)
    crud.log_event(db, log)
