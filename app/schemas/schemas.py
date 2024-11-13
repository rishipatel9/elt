from pydantic import BaseModel
from typing import Optional

class TransactionCreate(BaseModel):
    order_id: Optional[str]
    transaction_type: str
    payment_type: Optional[str]
    net_amount: Optional[float]
    invoice_amount: Optional[float]

class LogCreate(BaseModel):
    level: str
    message: str
    details: Optional[str] = None
