from .__init__ import *
from services.currency import process_rates, update_daily_rates

currency_router = APIRouter(prefix="/currency", tags=["currencies"])


@currency_router.post("/rates")
async def process_rates_endpoint(file: UploadFile = File(...)):
    try:
        processed_data = await process_rates(file)
        return {"message": "Rates processed successfully", "data": processed_data}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
    

@currency_router.put('/rates')
async def daily_rates():
    return update_daily_rates()



