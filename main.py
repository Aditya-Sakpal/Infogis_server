from fastapi import FastAPI
from app.routers.crud import router


app=FastAPI()
app.include_router(router)