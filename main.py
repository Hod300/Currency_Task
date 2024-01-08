from fastapi import FastAPI
from routes.currency import currency_router

app = FastAPI(title="Backend_Api", description="Algo Backend Server", version="1.0")


@app.get('/')
def check_status():
    return 'ok'


app.include_router(currency_router)