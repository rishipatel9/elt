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
def process_datasets(payment_df: pd.DataFrame, tax_report_df: pd.DataFrame) -> pd.DataFrame:
    try:
        payment_df.columns = payment_df.columns.str.title()
        tax_report_df.columns = tax_report_df.columns.str.title()
        
        if 'Transaction Type' in tax_report_df.columns:
            tax_report_df = tax_report_df[tax_report_df['Transaction Type'] != 'Cancel'].copy()
            tax_report_df['Transaction Type'] = tax_report_df['Transaction Type'].replace({
                'Refund': 'Return', 
                'FreeReplacement': 'Return'
            })
        else:
            print("Warning: 'Transaction Type' column not found in tax_report_df")
        
        if 'Type' in payment_df.columns:
            payment_df = payment_df[payment_df['Type'] != 'Transfer'].copy()
            payment_df.rename(columns={'Type': 'Payment Type'}, inplace=True)
            
            payment_df['Payment Type'] = payment_df['Payment Type'].replace({
                'Ajdustment': 'Order',  
                'FBA Inventory Fee': 'Order',
                'Fulfilment Fee': 'Order',
                'Service Fee': 'Order',
                'Refund': 'Return'
            })
            
            payment_df['Transaction Type'] = 'Payment'
        else:
            print("Warning: 'Type' column not found in payment_df")
        
        print("payment", payment_df.columns)
        print("tax", tax_report_df.columns)
        
        for df in [tax_report_df, payment_df]:
            if 'Item Description' in df.columns and 'Description' in df.columns:
                df['P_Description'] = df['Item Description'].fillna('') + ' ' + df['Description'].fillna('')
                df['P_Description'] = df['P_Description'].str.strip() 
                df.drop(['Item Description', 'Description'], axis=1, inplace=True)
            elif 'Item Description' in df.columns:
                df['P_Description'] = df['Item Description']
                df.drop(['Item Description'], axis=1, inplace=True)
            elif 'Description' in df.columns:
                df['P_Description'] = df['Description']
                df.drop(['Description'], axis=1, inplace=True)
        
        all_columns = list(set(tax_report_df.columns).union(set(payment_df.columns)))
        
        for col in all_columns:
            if col not in tax_report_df.columns:
                tax_report_df[col] = pd.NA
            if col not in payment_df.columns:
                payment_df[col] = pd.NA
        
        tax_report_df = tax_report_df[all_columns]
        payment_df = payment_df[all_columns]
        
        merged_df = pd.concat([tax_report_df, payment_df], ignore_index=True)
        merged_df.dropna(axis=1, how='all', inplace=True)
        
        return merged_df
    
    except Exception as e:
        print("Error processing datasets:", str(e))
        raise
