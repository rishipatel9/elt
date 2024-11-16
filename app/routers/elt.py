from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, Response
from sqlalchemy.orm import Session
from ..database import crud, database, models
from ..utils.transformations import load_datasets, process_datasets
from ..utils.logging_utils import log_info, log_error
import os
from ..schemas import schemas

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

        payment_df, tax_report_df = load_datasets(payment_path, tax_path)
        merged_df = process_datasets(payment_df, tax_report_df)

        transactions = []
        for _, row in merged_df.iterrows():
            if not row["Order Id"] or not row["Transaction Type"] or not row["Payment Type"]:
                continue
            transaction = schemas.TransactionCreate(
                order_id=row["Order Id"],
                transaction_type=row["Transaction Type"],
                payment_type=row["Payment Type"],
                net_amount=row.get("Net Amount", ), 
                invoice_amount=row.get("Invoice Amount", 0)  
            )
            transactions.append(transaction)

        # crud.bulk_create_transactions(db, transactions)

        print(f"CRUD performed successfully. {len(transactions)} transactions inserted.")
        log_info(db, f"Files uploaded and processed successfully. {len(transactions)} transactions processed.")
        return  Response(status_code=201, content="Files uploaded and processed successfully.")
    except Exception as e:
        log_error(db, "Error processing files", str(e))
        print("Error processing files:", str(e))
        return  Response(status_code=500, content="Error processing files.")
    
    
