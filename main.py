from fastapi import FastAPI, File, UploadFile, HTTPException
from currency import process_rates, update_daily_rates

app = FastAPI()

@app.post("/rates")
async def process_rates_endpoint(file: UploadFile = File(...)):
    try:
        
        processed_data = await process_rates(file)
        return {"message": "Rates processed successfully", "data": processed_data}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
    

@app.put('/rates')
async def daily_rates():
    return update_daily_rates()

