from fastapi import APIRouter
from app.router.v1.endpoints.crawler import router as crawler_router

router = APIRouter()

router.include_router(crawler_router, prefix="/crawler", tags=["crawler"])