from fastapi import APIRouter
from playwright.async_api import async_playwright

router = APIRouter()

@router.get("/screenshot")
async def take_screenshot(url: str = "https://www.google.com", filename: str = "screenshot.png"):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(viewport={"width": 1280, "height": 800})
            page = await context.new_page()
            await page.goto(url, wait_until="networkidle")
            await page.wait_for_load_state("domcontentloaded")
            await page.screenshot(path=filename)
            await browser.close()
        return {"message": "Screenshot taken"}
    
    except Exception as e:
        return {"message": f"Error: {e}"}