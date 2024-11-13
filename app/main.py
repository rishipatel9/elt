from fastapi import FastAPI
from .routers import elt
from .database import models, database

app = FastAPI()

# Create tables
models.Base.metadata.create_all(bind=database.engine)

# Include router
app.include_router(elt.router, prefix="/api/elt", tags=["ELT"])
