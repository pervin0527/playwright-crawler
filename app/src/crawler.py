import re
import asyncio
import pandas as pd
from typing import Optional
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient

from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from app.src.corp_code import search_company
from app.utils.time import get_current_korea_time
from app.utils.data import clean_account_name, clean_paragraph_text, extract_year_from_report_title
from app.utils.logging import logger


class FinancialStatementCrawler:
    INIT_URL = "https://dart.fss.or.kr/main.do"
    TARGET_SJ_LIST = [
        "연결재무제표", "재무제표",
        "연결재무상태표", "연결손익계산서", "연결포괄손익계산서", 
        "재무상태표", "손익계산서", "포괄손익계산서",
    ]

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None


    async def init_browser(self):
        logger.info(f"[init] playwright 브라우저 초기화 시작")
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-extensions',
                '--disable-software-rasterizer',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--disable-features=TranslateUI',
                '--disable-blink-features=AutomationControlled',
                '--window-size=1920,1080'
            ]
        )

        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        self.page = await self.context.new_page()
        logger.info(f"[init] 브라우저 초기화 완료")

        await self.page.goto(self.INIT_URL, wait_until='networkidle', timeout=60000)
        logger.info(f"[init] DART 페이지 접속 완료")

        await self.page.screenshot(path=f'/playwright-crawler/screenshots/00_init.png')

        return True
    

    async def search_company(self, company_name: str):
        logger.info(f"[search_company] 기업 검색 시작: {company_name}")

        search_input = self.page.locator('#textCrpNm2')
        await search_input.fill(company_name)
        await search_input.press('Enter')

        await self.page.wait_for_load_state('networkidle')
        await self.page.screenshot(path=f'/playwright-crawler/screenshots/01_query_input.png')

        await self.page.locator('#date7').click() ## 기간 선택
        await self.page.locator('#li_01 > label').click() ## 정기공시 클릭
        await self.page.locator('#divPublicTypeDetail_01 > ul > li:nth-child(1) > span > label').click() ## 사업보고서 클릭
        await self.page.locator('#searchForm > div.subSearchWrap > div.btnArea > a.btnSearch').click()

        ## 1초 대기
        await asyncio.sleep(1)
        await self.page.wait_for_load_state('networkidle')
        await self.page.screenshot(path=f'/playwright-crawler/screenshots/02_set_option.png')

        logger.info(f"[search_company] 기업 검색 완료: {company_name}")
        return True
    

    async def collect_report_list(self):
        logger.info(f"[collect_report_list] 보고서 목록 수집 시작")
        
        table_rows = self.page.locator('#tbody tr')
        row_count = await table_rows.count()
        logger.info(f"[collect_report_list] 보고서 목록 수집 완료: {row_count}개")

        reports = []
        all_rows = await table_rows.all()
        for i, row in enumerate(all_rows):
            try:
                # 각 td 요소 가져오기
                tds = row.locator('td')
                
                # 1. 인덱스 (첫 번째 td)
                index_text = await tds.nth(0).text_content()
                
                # 2. 회사명 (두 번째 td)
                company_name_raw = await tds.nth(1).text_content()
                # 불필요한 텍스트와 공백 제거
                company_name = company_name_raw.strip() if company_name_raw else ''
                # '유' 문자와 그 뒤의 공백들을 제거하고 실제 회사명만 추출
                if company_name.startswith('유'):
                    # '유' 이후의 공백과 탭을 제거하고 실제 회사명 추출
                    company_name = re.sub(r'^유\s+', '', company_name)
                    company_name = company_name.strip()
                
                # 3. 보고서명 (세 번째 td) - 제목과 URL 가져오기
                report_td = tds.nth(2)
                report_link = report_td.locator('a')
                report_title_raw = await report_link.text_content()
                report_title = report_title_raw.strip() if report_title_raw else ''
                report_url = await report_link.get_attribute('href')
                
                ## 보고서 제목에서 이름과 날짜 분리
                report_name = ''
                publish_date = ''
                if report_title:
                    # 정규식을 사용하여 '보고서명 (날짜)' 형태에서 분리
                    match = re.match(r'^(.+?)\s*\(([^)]+)\)\s*$', report_title)
                    if match:
                        report_name = match.group(1).strip()
                        publish_date = match.group(2).strip()
                    else:
                        # 괄호가 없는 경우 전체를 보고서명으로 사용
                        report_name = report_title

                if '제출기한연장신고서' in report_name:
                    continue

                report_data = {
                    'index': index_text.strip() if index_text else '',
                    'company_name': company_name,
                    'report_name': report_name,
                    'publish_date': publish_date,
                    'report_url': f"https://dart.fss.or.kr/{report_url}" if report_url else '',
                    'rcept_no': report_url.split('=')[-1]
                }
                
                reports.append(report_data)
                # logger.info(f"[collect_report_list] 행 {i+1}: {report_data['company_name']} - {report_data['report_name']} - {report_data['publish_date']}")
                
            except Exception as e:
                logger.error(f"[collect_report_list] 행 {i+1} 처리 중 오류: {str(e)}")
                continue
        
        logger.info(f"[collect_report_list] 총 {len(reports)}개 보고서 정보 수집 완료")
        return reports

    async def collect_financial_statements(self, company_name: str):
        logger.info(f"[collect_financial_statements] 재무제표 수집 시작: {company_name}")
        
        await self.init_browser()
        await self.search_company(company_name)
        report_list = await self.collect_report_list()
        
        logger.info(f"[collect_financial_statements] 총 {len(report_list)}개 보고서 정보 수집 완료")
        for report in report_list:
            logger.info((f"{report['company_name']} - {report['report_name']} - {report['publish_date']} - {report['report_url']} - {report['rcept_no']}"))