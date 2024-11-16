from pydantic import BaseModel
from typing import Optional

class TransactionCreate(BaseModel):
    order_id: str
    transaction_type: str
    payment_type: Optional[str] = None
    net_amount: Optional[float] = None  
    invoice_amount: Optional[float] = None 

class LogCreate(BaseModel):
    level: str
    message: str
    details: Optional[str] = None
