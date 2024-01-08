import pandas as pd
from datetime import datetime
from fastapi import HTTPException, UploadFile
from forex_python.converter import CurrencyRates
from io import BytesIO

RATE_FILE = 'data/rates.csv'
date_format='%m/%d/%Y'

# Load currency mapping
currency_df = pd.read_csv('data/currency.csv')

currency_mapping = {key: str(value) for key, value in zip(currency_df['EN'], currency_df['id'])}


async def process_rates(file: UploadFile):
    if not file.filename.endswith(('.csv', '.excel')):
       raise HTTPException(422, 'Unsupported file format')
    
    df = process_file('data/rates.csv', date_format)    
    new_rates = process_file(BytesIO(await file.read()), excel_file=file.filename.endswith('.excel'))
    
    # Combine all data
    combined_df = pd.concat([new_rates, df], sort=False).drop_duplicates(subset='Date', keep='first')
   
    if new_rates.empty:
        return 'data is up to date'
    
    
    combined_df = combined_df.groupby('Date').last().reset_index()

    all_dates = pd.date_range(start=combined_df['Date'].min(), end=combined_df['Date'].max())
    combined_df.set_index('Date', inplace=True)
    combined_df = combined_df.reindex(all_dates).rename_axis('Date').reset_index()
    
    for column in combined_df.columns[1:]:
        combined_df[column] = combined_df[column].ffill()

    cols = ['Date'] + [col for col in combined_df.columns if col != 'Date']
    combined_df = combined_df[cols].sort_values(by='Date', ascending=False)
    combined_df['Date'] = combined_df['Date'].dt.strftime(date_format)
    combined_df.to_csv(RATE_FILE, index=False, date_format=date_format)
    return combined_df.head().fillna(0).to_dict(orient='records')


def process_file(file, format='%d/%m/%Y', excel_file=False) -> pd.DataFrame:
    try:
        df = pd.read_csv(file) if not excel_file else pd.read_excel(file)
        df['Date'] = pd.to_datetime(df['Date'], format=format)
        df.columns = [currency_mapping.get(col, col) for col in df.columns]
        return df
    except ValueError:
        raise HTTPException(status_code=422, detail='Wrong format, enter a new one')




def update_daily_rates(base_currency='ILS') -> None:
    df = process_file('data/rates.csv', date_format)
    df['Date'] = df['Date'].dt.strftime(date_format)
    all_rates = get_all_exchange_rates(base_currency)
    converted_all_rates = {
        str(currency_mapping.get(col, col)): round(1 / all_rates.get(col, 0), 3)
        for col in all_rates if currency_mapping.get(col, col) in df.columns
    } | {'Date': datetime.now().strftime(date_format)}


    new_row = pd.Series(converted_all_rates, name=0)
    
    result_df = pd.concat([new_row.to_frame().T, df], ignore_index=True)
    
    result_df[df.columns].to_csv(RATE_FILE, index=False, date_format=date_format)
    return converted_all_rates

def get_all_exchange_rates(base_currency) -> dict:
    try:
        currency_rates = CurrencyRates()
        rates = currency_rates.get_rates(base_currency)
        return rates
    except Exception as e:
        print(f"Error fetching rates: {e}")
        return {}
