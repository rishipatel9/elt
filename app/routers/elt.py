from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlalchemy.orm import Session
from ..database import crud, database
from ..utils.transformations import load_datasets, process_datasets
from ..utils.logging_utils import log_info, log_error
import os


router = APIRouter()

@router.post("/upload")
async def upload_files(payment_file: UploadFile = File(...), tax_file: UploadFile = File(...), db: Session = Depends(database.get_db)):
    try:
        os.makedirs("temp", exist_ok=True)  
        payment_path = f"temp/{payment_file.filename}"
        tax_path = f"temp/{tax_file.filename}"
        
        with open(payment_path, "wb") as f:
            f.write(payment_file.file.read())
        with open(tax_path, "wb") as f:
            f.write(tax_file.file.read())

        log_info(db, f"Files saved successfully: {payment_path}, {tax_path}")

        print("Files uploaded successfully.")
        print("Files laoded successfully.")
        payment_df, tax_report_df = load_datasets(payment_path, tax_path)
        merged_df = process_datasets(payment_df, tax_report_df)
        
        # Insert merged data into the database
        for _, row in merged_df.iterrows():
            transaction = {
                "order_id": row.get("Order ID"),
                "transaction_type": row.get("Transaction Type"),
                "payment_type": row.get("Payment Type"),
                "net_amount": row.get("Net Amount"),
                "invoice_amount": row.get("Invoice Amount")
            }
            crud.create_transaction(db, transaction)

        log_info(db, "Files uploaded and processed successfully.")
        return {"status": "success"}
    
    except Exception as e:
        log_error(db, "Error processing files", str(e))
        raise HTTPException(status_code=500, detail="File processing failed.")
