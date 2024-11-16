import pandas as pd

def load_datasets(payment_path: str, tax_report_path: str):
    try:  
        payment_df = pd.read_csv(
            payment_path, 
            encoding='ISO-8859-1', 
            on_bad_lines='skip',
            lineterminator='\n'       
        )
        tax_report_df = pd.read_excel(tax_report_path, engine='openpyxl')

        return payment_df, tax_report_df
    except Exception as e:
        print("Error loading datasets:", str(e))
        raise

import pandas as pd
import numpy as np


def process_datasets(payment_df: pd.DataFrame, tax_report_df: pd.DataFrame) -> pd.DataFrame:
    """
    Process and merge datasets, then categorize and apply tolerance checks.
    """
    try:
        # Normalize column names
        payment_df.columns = payment_df.columns.str.title().str.strip()
        tax_report_df.columns = tax_report_df.columns.str.title().str.strip()

        # Process Transaction Type in tax_report_df
        if 'Transaction Type' in tax_report_df.columns:
            tax_report_df = tax_report_df[tax_report_df['Transaction Type'] != 'Cancel']
            tax_report_df.loc[:, 'Transaction Type'] = tax_report_df['Transaction Type'].replace({
                'Refund': 'Return',
                'FreeReplacement': 'Return'
            })


        # Process Payment DataFrame
        if 'Type' in payment_df.columns:
            payment_df = payment_df[payment_df['Type'] != 'Transfer']
            payment_df.rename(columns={'Type': 'Payment Type', 'Total': 'Net Amount'}, inplace=True)
            payment_df['Payment Type'] = payment_df['Payment Type'].replace({
                'Ajdustment': 'Order',
                'FBA Inventory Fee': 'Order',
                'Fulfilment Fee': 'Order',
                'Service Fee': 'Order',
                'Refund': 'Return'
            })
            payment_df['Transaction Type'] = 'Payment'

        # Convert columns to numeric where applicable
        payment_df['Net Amount'] = pd.to_numeric(payment_df.get('Net Amount', pd.NA), errors='coerce')
        tax_report_df.loc[:, 'Invoice Amount'] = pd.to_numeric(tax_report_df.get('Invoice Amount', pd.NA), errors='coerce')

        # Merge datasets
        merged_df = pd.concat([tax_report_df, payment_df], ignore_index=True).dropna(axis=1, how='all')

        # Group by 'Order Id' and aggregate
        if 'Order Id' in merged_df.columns:
            grouped = merged_df.groupby('Order Id', as_index=False).agg({
                'Transaction Type': 'first',
                'Payment Type': 'first',
                'Net Amount': 'sum',
                'Invoice Amount': 'sum'
            })
        else:
            raise KeyError("Missing required 'Order Id' column in merged data.")

        # Categorization function
        def categorize(row):
            order_id = row.get('Order Id', None)
            if pd.notna(order_id) and isinstance(order_id, str) and len(order_id) == 10:
                return 'Removal Order IDs'
            if row['Transaction Type'] == 'Return' and pd.notna(row['Invoice Amount']):
                return 'Return'
            if row['Transaction Type'] == 'Payment' and row['Net Amount'] < 0:
                return 'Negative Payout'
            if pd.notna(order_id) and pd.notna(row['Net Amount']) and pd.notna(row['Invoice Amount']):
                return 'Order & Payment Received'
            if pd.notna(order_id) and pd.notna(row['Net Amount']) and pd.isna(row['Invoice Amount']):
                return 'Order Not Applicable but Payment Received'
            if pd.notna(order_id) and pd.isna(row['Net Amount']) and pd.notna(row['Invoice Amount']):
                return 'Payment Pending'
            return 'Uncategorized'

        # Apply categorization logic
        grouped['Category'] = grouped.apply(categorize, axis=1)

        # Tolerance check function
        def tolerance_check(row):
            if pd.notna(row['Net Amount']) and pd.notna(row['Invoice Amount']) and row['Invoice Amount'] != 0:
                percentage = (row['Net Amount'] / row['Invoice Amount']) * 100
                if 0 < row['Net Amount'] < 300 and percentage > 50:
                    return 'Within Tolerance'
                elif 301 < row['Net Amount'] < 500 and percentage > 45:
                    return 'Within Tolerance'
                elif 501 < row['Net Amount'] < 900 and percentage > 43:
                    return 'Within Tolerance'
                elif 901 < row['Net Amount'] < 1500 and percentage > 38:
                    return 'Within Tolerance'
                elif row['Net Amount'] > 1500 and percentage > 30:
                    return 'Within Tolerance'
                return 'Tolerance Breached'
            return 'No Data'

        # Apply tolerance logic
        grouped['Tolerance Status'] = grouped.apply(tolerance_check, axis=1)

        # Save processed file
        output_path = "temp/merged_transactions.csv"
        grouped.to_csv(output_path, index=False)
        print(f"Processed file saved to {output_path}")
        return grouped

    except Exception as e:
        print(f"Error processing datasets: {str(e)}")
        return pd.DataFrame()
