import os
import asyncio

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from playwright.async_api import async_playwright
from app.router.v1.router import router as v1_router

app = FastAPI(
    title="Playwright API",
    description="Playwright Crawling API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

v1_app = FastAPI(
    title="Playwright API v1",
    description="Playwright Crawling API v1",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

v1_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

v1_app.include_router(v1_router)
app.mount("/api/v1", v1_app)


@app.get("/health_check")
async def health_check():
    return {"status": "ok"}