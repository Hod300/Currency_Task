from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import os

app = FastAPI()

# Directory where the CSV files are stored
file_directory = 'C:/Users/Peleg Mordechai/Desktop/Currency_Task'

# Read currency mapping and ensure keys are strings
currency_df = pd.read_csv(os.path.join(file_directory, 'currency.csv'))
currency_mapping = {str(key): value for key, value in zip(currency_df['id'], currency_df['EN'])}

def process_rates():
    # Step 1: Read rates and additional rates
    rates_df = pd.read_csv(os.path.join(file_directory, 'rates.csv'))
    additional_rates_df = pd.read_csv(os.path.join(file_directory, 'additional_rates.csv'))
    additional_rates_CAD_df = pd.read_csv(os.path.join(file_directory, 'additional_rates_CAD.csv'))

    # Step 2: Convert 'Date' columns to datetime
    rates_df['Date'] = pd.to_datetime(rates_df['Date'],format='%m/%d/%Y')
    additional_rates_df['Date'] = pd.to_datetime(additional_rates_df['Date'],format='%d/%m/%Y')
    additional_rates_CAD_df['Date'] = pd.to_datetime(additional_rates_CAD_df['Date'],format='%d/%m/%Y')

    # Step 3: Translate currency IDs to names
    rates_df.columns = [currency_mapping.get(col, col) for col in rates_df.columns]
    additional_rates_df.columns = [currency_mapping.get(col, col) for col in additional_rates_df.columns]
    additional_rates_CAD_df.columns = [currency_mapping.get(col, col) for col in additional_rates_CAD_df.columns]

    # Step 4: Combine all data
    combined_df = pd.concat([rates_df, additional_rates_df, additional_rates_CAD_df], sort=False).sort_values('Date',
                                                                                                              ascending=True)
    # Remove duplicate dates, keeping the last non-null value for each currency
    combined_df = combined_df.groupby('Date').last().reset_index()

    # Reindex to fill in missing dates
    all_dates = pd.date_range(start=combined_df['Date'].min(), end=combined_df['Date'].max())
    combined_df.set_index('Date', inplace=True)
    combined_df = combined_df.reindex(all_dates).rename_axis('Date').reset_index()

    # Step 5: Forward fill missing values for each currency column separately
    for column in combined_df.columns[1:]:  # Skip the 'Date' column
        combined_df[column] = combined_df[column].ffill()

    # Ensure 'Date' is the first column and sort by date in descending order
    cols = ['Date'] + [col for col in combined_df.columns if col != 'Date']
    combined_df = combined_df[cols].sort_values(by='Date', ascending=False)

    # Save the processed data to a new CSV file
    processed_file_path = os.path.join(file_directory, 'processed_rates.csv')
    combined_df.to_csv(processed_file_path, index=False, date_format='%d/%m/%Y')

    return combined_df

@app.get("/process-rates")
async def process_rates_endpoint():
    try:
        processed_data = process_rates()
        return JSONResponse(status_code=200, content={"message": "Rates processed successfully", "data": processed_data.head().to_dict(orient='records')})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
