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
    except FileNotFoundError as e:
        print(f"File not found: {e}")
        raise
    except pd.errors.ParserError as e:
        print(f"Error parsing file: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error loading datasets: {e}")
        raise

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df.columns = df.columns.str.title().str.strip()
        return df
    except AttributeError as e:
        print(f"Error normalizing column names: {e}")
        raise

def process_transaction_type(df: pd.DataFrame) -> pd.DataFrame:
    try:
        if 'Transaction Type' in df.columns:
            df = df[df['Transaction Type'] != 'Cancel']
            df.loc[:, 'Transaction Type'] = df['Transaction Type'].replace({
                'Refund': 'Return',
                'FreeReplacement': 'Return'
            })
        return df
    except KeyError as e:
        print(f"Column not found for transaction type processing: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error processing transaction type: {e}")
        raise


def process_payment_type(df: pd.DataFrame) -> pd.DataFrame:
    try:
        if 'Type' in df.columns:
            df = df[df['Type'] != 'Transfer']
            df.rename(columns={'Type': 'Payment Type', 'Total': 'Net Amount'}, inplace=True)
            df.loc[:, 'Payment Type'] = df['Payment Type'].replace({
                'Ajdustment': 'Order',
                'FBA Inventory Fee': 'Order',
                'Fulfilment Fee': 'Order',
                'Service Fee': 'Order',
                'Refund': 'Return'
            })
            df.loc[:, 'Transaction Type'] = 'Payment'
        return df
    except KeyError as e:
        print(f"Column not found for payment type processing: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error processing payment type: {e}")
        raise

def convert_to_numeric(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    try:
        for column in columns:
            df.loc[:, column] = pd.to_numeric(df.get(column, pd.NA), errors='coerce')
        return df
    except ValueError as e:
        print(f"Error converting columns to numeric: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error during numeric conversion: {e}")
        raise

def merge_datasets(payment_df: pd.DataFrame, tax_report_df: pd.DataFrame) -> pd.DataFrame:
    try:
        merged_df = pd.concat([tax_report_df, payment_df], ignore_index=True).dropna(axis=1, how='all')
        return merged_df
    except Exception as e:
        print(f"Error merging datasets: {e}")
        raise

def group_and_aggregate(merged_df: pd.DataFrame) -> pd.DataFrame:
    try:
        if 'Order Id' in merged_df.columns:
            grouped = merged_df.groupby('Order Id', as_index=False).agg({
                'Transaction Type': 'first',
                'Payment Type': 'first',
                'Net Amount': 'sum',
                'Invoice Amount': 'sum'
            })
            return grouped
        else:
            raise KeyError("Missing required 'Order Id' column in merged data.")
    except KeyError as e:
        print(f"Error in group and aggregate: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error during grouping and aggregation: {e}")
        raise

def categorize(row) -> str:
    try:
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
    except KeyError as e:
        print(f"Error in categorization: Missing column {e}")
        raise
    except Exception as e:
        print(f"Unexpected error in categorization: {e}")
        raise

def apply_tolerance_check(row) -> str:
    try:
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
    except KeyError as e:
        print(f"Error in tolerance check: Missing column {e}")
        raise
    except Exception as e:
        print(f"Unexpected error in tolerance check: {e}")
        raise
def process_datasets(payment_df: pd.DataFrame, tax_report_df: pd.DataFrame) -> pd.DataFrame:
    try:
        payment_df = normalize_column_names(payment_df)
        tax_report_df = normalize_column_names(tax_report_df)

        payment_df = process_payment_type(payment_df)
        tax_report_df = process_transaction_type(tax_report_df)

        payment_df = convert_to_numeric(payment_df, ['Net Amount'])
        tax_report_df = convert_to_numeric(tax_report_df, ['Invoice Amount'])

        payment_df.replace(0, pd.NA, inplace=True)
        tax_report_df.replace(0, pd.NA, inplace=True)

        merged_df = merge_datasets(payment_df, tax_report_df)

        grouped = group_and_aggregate(merged_df)

        grouped['Category'] = grouped.apply(categorize, axis=1)
        grouped['Tolerance Status'] = grouped.apply(apply_tolerance_check, axis=1)

        output_path = "temp/merged_transactions.csv"
        grouped.to_csv(output_path, index=False)
        print(f"Processed file saved to {output_path}")
        return grouped
    except Exception as e:
        print(f"Error processing datasets: {str(e)}")
        return pd.DataFrame()