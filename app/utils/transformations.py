import pandas as pd

def load_datasets(payment_path: str, tax_report_path: str):
    payment_df = pd.read_csv(payment_path)
    tax_report_df = pd.read_excel(tax_report_path)
    return payment_df, tax_report_df

def process_datasets(payment_df: pd.DataFrame, tax_report_df: pd.DataFrame) -> pd.DataFrame:
    tax_report_df = tax_report_df[tax_report_df['Transaction Type'] != 'Cancel']
    tax_report_df['Transaction Type'] = tax_report_df['Transaction Type'].replace({'Refund': 'Return', 'FreeReplacement': 'Return'})
    
    payment_df = payment_df[payment_df['Type'] != 'Transfer']
    payment_df.rename(columns={'Type': 'Payment Type'}, inplace=True)
    payment_df['Transaction Type'] = 'Payment'
    
    merged_df = pd.concat([tax_report_df, payment_df], ignore_index=True)
    return merged_df
