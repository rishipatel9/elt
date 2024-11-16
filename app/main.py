from fastapi import FastAPI
from .routers import elt
from .database import models, database

app = FastAPI()

models.Base.metadata.create_all(bind=database.engine)

app.include_router(elt.router, prefix="/api/elt", tags=["ELT"])
