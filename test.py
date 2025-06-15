import os
import sys
import asyncio

from datetime import datetime
from playwright.async_api import async_playwright

async def search_company(corp_name):
    """
    DART 웹사이트에서 기업 정보를 검색하는 함수
    
    Args:
        corp_name (str): 검색할 기업 이름
    """
    async with async_playwright() as p:
        # 브라우저 실행 (크로미움 기반)
        print('브라우저를 실행합니다...')
        browser = await p.chromium.launch(
            headless=False,  # 실행 과정을 눈으로 확인하기 위해 headless 모드를 끕니다
            slow_mo=100,     # 각 동작 사이에 100ms 지연을 줍니다 (동작 확인용)
        )
        
        # 새 페이지(탭) 열기
        page = await browser.new_page()
        
        try:
            # DART 메인 페이지로 이동
            print('DART 웹사이트에 접속합니다...')
            await page.goto('https://dart.fss.or.kr/main.do', {
                'wait_until': 'networkidle',  # 페이지 로드가 완료될 때까지 기다림
                'timeout': 60000,            # 최대 60초 대기
            })
            
            # 현재 시간을 파일명에 포함 (중복 방지)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # 페이지 로드 후 스크린샷 촬영
            print('페이지 로드 완료. 스크린샷을 촬영합니다...')
            await page.screenshot(path=f'dart_main_page_{timestamp}.png')
            
            # 검색창 요소 찾기 (#textCrpNm2)
            print(f"검색창을 찾아 '{corp_name}'을(를) 입력합니다...")
            search_input = page.locator('#textCrpNm2')
            
            # 검색창이 존재하는지 확인
            if await search_input.count() == 0:
                raise Exception('검색창을 찾을 수 없습니다. 선택자(#textCrpNm2)를 확인해주세요.')
            
            # 검색창에 기업명 입력
            await search_input.fill(corp_name)
            
            # Enter 키 입력하여 검색 실행
            print('검색을 실행합니다...')
            await search_input.press('Enter')
            
            # 검색 결과 로딩 대기
            print('검색 결과가 로드될 때까지 기다립니다...')
            await page.wait_for_load_state('networkidle')
            
            # 검색 결과 페이지 스크린샷 촬영
            print('검색 완료. 결과 페이지의 스크린샷을 촬영합니다...')
            safe_corp_name = corp_name.replace('/', '_').replace('\\', '_')
            await page.screenshot(path=f'dart_search_result_{safe_corp_name}_{timestamp}.png')
            
            print('크롤링이 성공적으로 완료되었습니다.')
            
            # 여기에 추가적인 데이터 추출 로직을 구현할 수 있습니다
            # 예: 검색 결과에서 기업 정보 추출, 공시 목록 가져오기 등
            
        except Exception as e:
            print(f'오류가 발생했습니다: {e}')
        finally:
            # 브라우저 종료
            print('브라우저를 종료합니다...')
            await browser.close()

async def main():
    """메인 함수"""
    # 명령줄 인자 가져오기
    if len(sys.argv) > 1:
        corp_name = sys.argv[1]
    else:
        corp_name = '삼성전자'  # 기본값은 '삼성전자'
    
    await search_company(corp_name)

if __name__ == "__main__":
    asyncio.run(main())