from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

# Internal imports
from services import setup_logging, create_visa_faq_index, create_bug_report_index
from constants import uvicorn_host, uvicorn_port
import conversations
import batchprocessing
import fetchvisainfo
import retrievePackages
from batchprocessing import schedule_daily_job
import campaigns
import convostats

# -----------------------------------------------------------------------
# Setup Logging
# -----------------------------------------------------------------------
setup_logging()

# -----------------------------------------------------------------------
# Define FastAPI lifespan context to replace @app.on_event("startup")
# -----------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    create_visa_faq_index()
    create_bug_report_index()
    await schedule_daily_job()
    yield  # App will run here
    # Add any shutdown/cleanup logic here if needed

# -----------------------------------------------------------------------
# Initialize FastAPI App
# -----------------------------------------------------------------------
app = FastAPI(lifespan=lifespan)
router = APIRouter()

# -----------------------------------------------------------------------
# Add CORS Middleware
# -----------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------
# Register Routers
# -----------------------------------------------------------------------
app.include_router(retrievePackages.router, prefix="", tags=["TCIL Elastic Search Retrieval End Points"])
app.include_router(fetchvisainfo.router, prefix="", tags=["TCIL Visa End Points"])
app.include_router(conversations.router, prefix="", tags=["TCIL Chat History End Points"])
app.include_router(batchprocessing.router, prefix="", tags=["TCIL Batch Processing End Point"])
app.include_router(campaigns.router, prefix="", tags=["TCIL Campaign End Points"])
app.include_router(convostats.router, prefix="", tags=["TCIL Conversation Statistics End Points"])  
# -----------------------------------------------------------------------
# Run App (Only in standalone script mode)
# -----------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=uvicorn_host, port=uvicorn_port)
