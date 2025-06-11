from fastapi import FastAPI, BackgroundTasks
from playwright.async_api import async_playwright
import asyncio
import os
from pathlib import Path

app = FastAPI(title="Playwright API")

# 스크린샷 저장 디렉토리 생성
# 환경 변수에서 경로를 가져오거나 기본값으로 현재 작업 디렉토리 사용
SCREENSHOT_DIR = os.environ.get("SCREENSHOT_DIR", os.path.join(os.getcwd(), "screenshots"))
Path(SCREENSHOT_DIR).mkdir(parents=True, exist_ok=True)

@app.get("/")
async def read_root():
    return {"message": "Playwright API is running"}

@app.get("/screenshot")
async def take_screenshot(url: str = "https://www.google.com", filename: str = "screenshot.png", search_query: str = "@https://dart.fss.or.kr/"):
    """
    웹 페이지의 스크린샷을 찍는 API 엔드포인트
    Google 검색 기능 추가
    """
    # 파일 경로 설정
    path = os.path.join(SCREENSHOT_DIR, filename)
    print(f"Screenshot will be saved to: {path}")
    
    try:
        async with async_playwright() as p:
            # headless 모드로 변경
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(viewport={"width": 1280, "height": 800})
            page = await context.new_page()
            
            # Google로 이동
            print(f"Navigating to {url}")
            await page.goto(url, wait_until="networkidle")
            
            # 페이지가 로드될 때까지 잠시 기다림
            await page.wait_for_load_state("domcontentloaded")
            
            try:
                # 정확한 CSS 선택자를 사용하여 검색창 대기
                await page.wait_for_selector('#APjFqb', timeout=10000)
                
                # 검색창 찾아서 검색어 입력
                await page.fill('#APjFqb', search_query)
                
                # 검색 버튼 클릭 또는 Enter 키 입력
                await page.press('#APjFqb', "Enter")
                
                # 검색 결과가 로드될 때까지 대기
                await page.wait_for_load_state("networkidle")
            except Exception as search_err:
                print(f"Search failed: {search_err}. Taking screenshot of current page.")
                # 검색에 실패하더라도 현재 페이지의 스크린샷은 촬영
            
            # 스크린샷 촬영
            await page.screenshot(path=path)
            await browser.close()
            print(f"Screenshot saved to: {path}")
            
        # 파일이 생성되었는지 확인
        if os.path.exists(path):
            file_size = os.path.getsize(path)
            return {"status": "Screenshot saved", "path": path, "filename": filename, "size": file_size}
        else:
            return {"status": "Error", "message": f"Screenshot file not created at {path}"}
    except Exception as e:
        print(f"Error taking screenshot: {str(e)}")
        return {"status": "Error", "message": str(e)}

@app.get("/screenshots")
async def list_screenshots():
    """
    저장된 스크린샷 목록 조회
    """
    files = [f for f in os.listdir(SCREENSHOT_DIR) if os.path.isfile(os.path.join(SCREENSHOT_DIR, f))]
    return {"screenshots": files}

@app.get("/health")
async def health_check():
    """
    서비스 상태 확인
    """
    return {"status": "healthy", "playwright_env": os.environ.get("PLAYWRIGHT_BROWSERS_PATH")}