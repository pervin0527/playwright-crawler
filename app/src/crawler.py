import os
import gc
import time
import psutil
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


class PlaywrightFinancialStatementCrawler:
    INIT_URL = "https://dart.fss.or.kr/main.do"
    TARGET_SJ_LIST = [
        "연결재무제표", "재무제표",
        "연결재무상태표", "연결손익계산서", "연결포괄손익계산서", 
        "재무상태표", "손익계산서", "포괄손익계산서",
    ]
    
    def __init__(self, corp_name: str, stock_code: str, mongo_client=None, corp_code: Optional[str] = None, corp_type_value: str = "all", retry_count: int = 3):
        self.corp_name = corp_name
        self.stock_code = stock_code
        self.corp_code = corp_code if corp_code else None
        self.corp_type_value = corp_type_value
        self.retry_count = retry_count
        self.timeout = 30000  # Playwright는 밀리초 단위, 30초
        
        # 스크린샷 저장 디렉토리
        self.screenshot_dir = "/playwright-crawler/screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        
        # 법인 유형 코드 매핑
        self.corp_type_map = {
            "all": "전체",
            "P": "유가증권시장",
            "A": "코스닥시장",
            "N": "코넥스시장",
            "E": "기타법인"
        }

        # MongoDB 클라이언트 설정
        if mongo_client:
            if isinstance(mongo_client, AsyncIOMotorClient):
                logger.info("AsyncIOMotorClient를 동기식 MongoClient로 변환합니다")
                mongodb_url = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
                self.mongo_client = MongoClient(mongodb_url)
            else:
                self.mongo_client = mongo_client
        else:
            mongodb_url = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
            logger.info(f"새 MongoClient 생성: {mongodb_url}")
            self.mongo_client = MongoClient(mongodb_url)

        # Playwright 관련 객체들
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    async def _create_browser_context(self):
        """Playwright 브라우저 컨텍스트를 생성합니다."""
        try:
            self.playwright = await async_playwright().start()
            
            # Chromium 브라우저 실행 (Docker 환경에서 안정적)
            self.browser = await self.playwright.chromium.launch(
                headless=True,  # 헤드리스 모드
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
            
            # 새 컨텍스트 생성
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36'
            )
            
            # 새 페이지 생성
            self.page = await self.context.new_page()
            
            # 기본 타임아웃 설정
            self.page.set_default_timeout(self.timeout)
            
            logger.info("Playwright 브라우저 컨텍스트 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"브라우저 컨텍스트 생성 실패: {e}")
            await self._cleanup_browser()
            return False

    async def _cleanup_browser(self):
        """브라우저 리소스를 정리합니다."""
        try:
            if self.page:
                await self.page.close()
                self.page = None
                logger.info("페이지 종료 완료")
                
            if self.context:
                await self.context.close()
                self.context = None
                logger.info("컨텍스트 종료 완료")
                
            if self.browser:
                await self.browser.close()
                self.browser = None
                logger.info("브라우저 종료 완료")
                
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
                logger.info("Playwright 정리 완료")
                
        except Exception as e:
            logger.error(f"브라우저 정리 중 오류: {e}")
        
        # 가비지 컬렉션 실행
        gc.collect()
        
        # 메모리 사용량 로깅
        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            logger.info(f"정리 후 메모리 사용량: {memory_info.rss / 1024 / 1024:.2f} MB")
        except Exception as e:
            logger.error(f"메모리 사용량 확인 실패: {e}")

    async def _take_screenshot(self, step_name: str):
        """단계별 스크린샷을 촬영합니다."""
        try:
            timestamp = int(time.time())
            filename = f"{step_name}_{self.corp_name}_{timestamp}.png"
            filepath = os.path.join(self.screenshot_dir, filename)
            
            await self.page.screenshot(path=filepath, full_page=True)
            logger.info(f"스크린샷 저장: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"스크린샷 촬영 실패: {e}")
            return None

    async def _navigate_to_dart_main(self):
        """DART 메인 페이지로 이동합니다."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                logger.info(f"DART 메인 페이지 로드 시도 {attempt + 1}/{max_retries}")
                
                # 페이지 로드
                await self.page.goto(self.INIT_URL, wait_until="domcontentloaded")
                
                # layoutNotice 요소가 로드될 때까지 대기
                await self.page.wait_for_selector(".layoutNotice", timeout=15000)
                
                # 페이지 로드 완료 대기
                await self.page.wait_for_load_state("networkidle")
                
                # 스크린샷 촬영
                await self._take_screenshot("01_dart_main_loaded")
                
                logger.info(f"DART 메인 페이지 로드 성공. URL: {self.page.url}")
                return True
                
            except PlaywrightTimeoutError as e:
                logger.warning(f"페이지 로드 시도 {attempt + 1} 실패 (타임아웃): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(3)
                    continue
                else:
                    raise
                    
            except Exception as e:
                logger.warning(f"페이지 로드 시도 {attempt + 1} 실패: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(3)
                    continue
                else:
                    raise
        
        return False

    async def _search_corp_name(self, corp_name: str):
        """기업명을 검색합니다."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                logger.info(f"기업명 검색 시도 {attempt + 1}/{max_retries}: {corp_name}")
                
                # 검색 입력창 요소 대기 (locator 사용)
                search_input = self.page.locator("#textCrpNm2")
                await search_input.wait_for(timeout=10000)
                
                # 기존 텍스트 클리어 후 입력
                await search_input.clear()
                await asyncio.sleep(0.5)
                await search_input.fill(corp_name)
                await asyncio.sleep(0.5)
                
                # 스크린샷 촬영 (검색어 입력 후)
                await self._take_screenshot("02_search_input_filled")
                
                # Enter 키 입력으로 검색 실행
                await search_input.press("Enter")
                
                # 검색 결과 로드 대기 (2초)
                await asyncio.sleep(2)
                
                # 스크린샷 촬영 (검색 결과)
                await self._take_screenshot("03_search_results")
                
                logger.info(f"기업명 검색 성공: {corp_name}")
                return True
                
            except PlaywrightTimeoutError as e:
                logger.warning(f"기업명 검색 시도 {attempt + 1} 실패 (타임아웃): {e}")
                if attempt < max_retries - 1:
                    logger.info("페이지 새로고침 후 재시도...")
                    await asyncio.sleep(2)
                    await self.page.reload()
                    await asyncio.sleep(3)
                else:
                    logger.error("기업명 검색 최대 재시도 횟수 초과")
                    raise
                    
            except Exception as e:
                logger.warning(f"기업명 검색 시도 {attempt + 1} 실패: {e}")
                if attempt < max_retries - 1:
                    logger.info("페이지 새로고침 후 재시도...")
                    await asyncio.sleep(2)
                    await self.page.reload()
                    await asyncio.sleep(3)
                else:
                    logger.error("기업명 검색 최대 재시도 횟수 초과")
                    raise
        
        return False

    async def _handle_company_selection(self):
        """검색 결과에서 기업을 선택합니다."""
        try:
            # 기업 선택 창이 표시되는지 확인
            corp_info_window = self.page.locator("#winCorpInfo")
            is_visible = await corp_info_window.is_visible()
            
            logger.info(f"기업 선택 창 표시 여부: {is_visible}")
            
            if is_visible:
                # 스크린샷 촬영 (기업 선택 창)
                await self._take_screenshot("04_company_selection_window")
                
                # 검색된 기업 목록에서 대상 기업 찾기
                search_pop = self.page.locator(".searchPop")
                cont_wrap = search_pop.locator(".contWrap")
                list_contents = cont_wrap.locator("#corpListContents")
                tbl_wrap = list_contents.locator(".tbLWrap")
                tbl_inner = tbl_wrap.locator(".tbLInner")
                table = tbl_inner.locator("table")
                tbody = table.locator("tbody")
                company_rows = await tbody.locator("tr").all()
                
                logger.info(f"검색된 기업 수: {len(company_rows)}")
                
                target_found = False
                for row in company_rows:
                    cells = await row.locator("td").all()
                    
                    # 기업명 (두 번째 열, 첫 번째 문자 제외)
                    corp_name_cell = await cells[1].text_content()
                    candidate_corp_name = corp_name_cell[1:] if corp_name_cell else ""
                    
                    # 종목코드 (네 번째 열)
                    stock_code_cell = await cells[3].text_content()
                    candidate_stock_code = stock_code_cell.strip() if stock_code_cell else ""
                    
                    logger.info(f"후보 기업: {candidate_corp_name}, 종목코드: {candidate_stock_code}")
                    
                    # 대상 기업과 일치하는지 확인
                    if (self.corp_name == candidate_corp_name and 
                        self.stock_code == candidate_stock_code):
                        
                        # 체크박스 선택
                        checkbox = row.locator("td input[type='checkbox']").first
                        await checkbox.check()
                        
                        # 선택 버튼 클릭
                        btn_area = cont_wrap.locator(".btnArea")
                        select_button = btn_area.locator(".btnSB").first
                        await select_button.click()
                        
                        # 선택 완료 대기
                        await asyncio.sleep(1)
                        
                        # 스크린샷 촬영 (기업 선택 완료)
                        await self._take_screenshot("05_company_selected")
                        
                        target_found = True
                        logger.info(f"대상 기업 선택 완료: {self.corp_name}")
                        break
                
                if not target_found:
                    raise Exception(f"검색 결과에서 대상 기업을 찾을 수 없습니다: {self.corp_name} ({self.stock_code})")
            
            return True
            
        except Exception as e:
            logger.error(f"기업 선택 처리 중 오류: {e}")
            await self._take_screenshot("05_company_selection_error")
            raise

    async def _set_search_condition(self):
        """검색 조건을 설정합니다."""
        try:
            logger.info("검색 조건 설정 시작")
            
            # Selenium 코드와 동일한 구조로 요소 찾기
            sub_page_bg = self.page.locator(".subPageBg")
            container = sub_page_bg.locator("#container")
            contents_wrap = container.locator("#contentsWrap")
            contents = contents_wrap.locator("#contents")
            page_elem = contents.locator("#page")
            search_form = page_elem.locator("#searchForm")
            sub_search_wrap = search_form.locator(".subSearchWrap")
            sub_search = sub_search_wrap.locator(".subSearch")
            
            ul = sub_search.locator("ul")
            lis = await ul.locator("li").all()
            
            # 기간 설정 - lis[2]
            period_wrap = lis[2]
            rwrap = period_wrap.locator(".rWrap")
            date_select = rwrap.locator(".dateSelect")
            date_btns = await date_select.locator(".btnDate").all()
            
            # 마지막 버튼 클릭 (10년)
            if date_btns:
                await date_btns[-1].click()
                logger.info("기간 설정 완료: 최근 10년")
            
            # 공시유형 설정 - lis[3]
            disclosure_type = lis[3]
            sub_check = disclosure_type.locator("#subCheck")
            span = sub_check.locator("span")
            ul_disclosure = span.locator("ul")
            lis_disclosure = await ul_disclosure.locator("li").all()
            await lis_disclosure[0].click()
            logger.info("공시유형 설정 완료: 사업보고서")
            
            # 세부 옵션 설정 (정정공시 포함)
            detail_check_wrap = sub_search_wrap.locator("#detailCheckWrap")
            detail_check = detail_check_wrap.locator(".detailCheck")
            ul_detail = detail_check.locator("ul")
            lis_detail = await ul_detail.locator("li").all()
            span_check = lis_detail[0].locator(".frmCheck")
            label = span_check.locator("label")
            await label.click()
            logger.info("세부 옵션 설정 완료: 정정공시 포함")
            
            # 검색 버튼 클릭
            btn_area = sub_search_wrap.locator(".btnArea")
            btn_search = btn_area.locator(".btnSearch")
            await btn_search.click()
            
            # 검색 결과 로드 대기
            await asyncio.sleep(1)
            
            logger.info("검색 조건 설정 및 검색 실행 완료")
            return True
            
        except Exception as e:
            logger.error(f"검색 조건 설정 중 오류 발생: {e}")
            await self._take_screenshot("06_search_conditions_error")
            raise

    async def get_fs_list(self):
        """재무제표 목록을 가져옵니다."""
        try:
            # 페이지 구조에 따른 정확한 선택자 사용
            page_elem = self.page.locator("#page")
            list_contents = page_elem.locator("#listContents")
            tb_list_inner = list_contents.locator(".tbListInner")
            tb_list = tb_list_inner.locator(".tbList")
            
            # 결과 테이블이 로드될 때까지 대기
            tbody = tb_list.locator("#tbody")
            await tbody.wait_for(timeout=10000)
            
            # 결과 행들 가져오기
            result_rows = await tbody.locator("tr").all()
            logger.info(f'사업보고서 개수: {len(result_rows)}')
            
            # 데이터가 없는 경우 확인
            if len(result_rows) == 1:
                first_cell = result_rows[0].locator("td").first
                cell_class = await first_cell.get_attribute("class")
                if cell_class and "no_data" in cell_class:
                    logger.warning(f"{self.corp_name} (기업코드: {self.corp_code}, 종목코드: {self.stock_code})의 사업보고서가 조회되지 않습니다.")
                    return [], []
            
            public_year_list = []
            fs_url_list = []
            
            for row in result_rows:
                cells = await row.locator("td").all()
                
                # 세 번째 열에서 제목과 URL 추출
                if len(cells) > 2:
                    title_cell = cells[2]
                    link = title_cell.locator("a").first
                    title = await link.text_content()
                    url = await link.get_attribute("href")
                    
                    # 연도 추출
                    public_year = extract_year_from_report_title(title)
                    public_year_list.append(public_year)
                    fs_url_list.append(url)
            
            # 스크린샷 촬영 (재무제표 목록)
            await self._take_screenshot("08_financial_statement_list")
            
            logger.info(f"공시 연도 목록: {public_year_list}")
            logger.info(f"재무제표 URL 목록 수: {len(fs_url_list)}")
            
            return public_year_list, fs_url_list
            
        except Exception as e:
            logger.error(f"재무제표 목록 조회 중 오류: {e}")
            await self._take_screenshot("08_fs_list_error")
            raise

    async def save_company_info_to_mongo(self, public_year_list, fs_url_list, industry_levels=None):
        """회사 정보를 MongoDB에 저장합니다."""
        try:
            company_collection = self.mongo_client["dart"]["COMPANY"]
            logger.info("company 컬렉션 접근 성공")
            
            # 기업 존재 여부 확인
            existing_company = company_collection.find_one({"stock_code": self.stock_code})
            
            if existing_company is None:
                # 신규 기업인 경우 bsns_years와 rcept_numbers 리스트를 생성
                insert_data = {
                    "corp_name": self.corp_name,
                    "stock_code": self.stock_code,
                    "corp_code": self.corp_code,
                    "corp_type_value": self.corp_type_value,
                    "corp_type_name": self.corp_type_map.get(self.corp_type_value, "알 수 없음"),
                    "bsns_years": [str(year) for year in public_year_list],
                    "rcept_numbers": [url.split("=")[-1] for url in fs_url_list],
                    "status": "success",
                    "message": "재무제표 URL 조회 성공",
                    "created_at": get_current_korea_time()
                }
                
                # 산업 분류 정보 추가
                if industry_levels:
                    insert_data.update(industry_levels)
                
                result = company_collection.insert_one(insert_data)
                logger.info(f"새 기업 정보 저장: {self.corp_name}, ID: {result.inserted_id}")
                
            else:
                # 기존 기업인 경우 bsns_years와 rcept_numbers 리스트에 값 추가
                update_data = {
                    "$addToSet": {
                        "bsns_years": {"$each": [str(year) for year in public_year_list]},
                        "rcept_numbers": {"$each": [url.split("=")[-1] for url in fs_url_list]}
                    },
                    "$set": {
                        "corp_type_value": self.corp_type_value,
                        "corp_type_name": self.corp_type_map.get(self.corp_type_value, "알 수 없음"),
                        "status": "success",
                        "message": "재무제표 URL 조회 성공",
                        "updated_at": get_current_korea_time()
                    }
                }
                
                # 산업 분류 정보 추가
                if industry_levels:
                    for level_key, level_value in industry_levels.items():
                        if level_value and level_value != "없음":  # 값이 있는 경우에만 업데이트
                            update_data["$set"][level_key] = level_value
                
                result = company_collection.update_one(
                    {"stock_code": self.stock_code},
                    update_data
                )
                logger.info(f"기업 정보 업데이트: {self.corp_name}, modified: {result.modified_count}")
                
        except Exception as e:
            logger.error(f"MongoDB COMPANY 컬렉션 저장 오류: {e}")
            # 실패 상태로 업데이트
            try:
                company_collection = self.mongo_client["dart"]["COMPANY"]
                company_collection.update_one(
                    {"stock_code": self.stock_code},
                    {"$set": {
                        "corp_name": self.corp_name,
                        "stock_code": self.stock_code,
                        "corp_code": self.corp_code,
                        "status": "failed",
                        "message": f"MongoDB 저장 오류: {str(e)}",
                        "updated_at": get_current_korea_time()
                    }},
                    upsert=True
                )
            except Exception as mongo_error:
                logger.error(f"실패 상태 저장 중 오류: {mongo_error}")

    async def save_failed_status_to_mongo(self, error_message):
        """실패 상태를 MongoDB에 저장합니다."""
        try:
            company_collection = self.mongo_client["dart"]["COMPANY"]
            result = company_collection.update_one(
                {"stock_code": self.stock_code},
                {"$set": {
                    "corp_name": self.corp_name,
                    "stock_code": self.stock_code,
                    "corp_code": self.corp_code,
                    "status": "failed",
                    "message": error_message,
                    "updated_at": get_current_korea_time()
                }},
                upsert=True
            )
            logger.info(f"기업 정보 실패 상태로 등록: {self.corp_name}, upsert: {result.upserted_id is not None}")
        except Exception as e:
            logger.error(f"실패 상태 저장 중 오류: {e}")

    async def initialize_and_search(self):
        """초기화 및 기업 검색을 수행합니다."""
        try:
            # 기업 정보 검색
            company_info = search_company(self.corp_name, self.corp_type_value)
            
            industry_levels = {}
            if company_info:
                if company_info.get('corp_code'):
                    self.corp_code = company_info['corp_code']
                if self.stock_code is None and company_info.get('stock_code'):
                    self.stock_code = company_info['stock_code']
                    
                # 산업 분류 정보 추출
                industry_levels = {
                    "level1": company_info.get('level1', '') if not pd.isna(company_info.get('level1', '')) else "없음",
                    "level2": company_info.get('level2', '') if not pd.isna(company_info.get('level2', '')) else "없음",
                    "level3": company_info.get('level3', '') if not pd.isna(company_info.get('level3', '')) else "없음",
                    "level4": company_info.get('level4', '') if not pd.isna(company_info.get('level4', '')) else "없음",
                    "level5": company_info.get('level5', '') if not pd.isna(company_info.get('level5', '')) else "없음"
                }
            else:
                logger.warning(f"산업 분류 정보를 찾을 수 없습니다: {self.corp_name}, {self.stock_code}")
            
            # 주식코드가 없는 경우 중단
            if self.stock_code is None:
                error_msg = "주식코드를 찾을 수 없습니다"
                logger.error(f"주식코드가 없어 크롤링을 중단합니다: {self.corp_name}")
                await self.save_failed_status_to_mongo(error_msg)
                return False, error_msg, [], []
            
            logger.info(f"기업명: {self.corp_name}, 기업코드: {self.corp_code}, 주식코드: {self.stock_code}")
            logger.info(f"산업 분류 정보: {industry_levels}")
            
            # 브라우저 컨텍스트 생성
            if not await self._create_browser_context():
                error_msg = "브라우저 컨텍스트 생성 실패"
                await self.save_failed_status_to_mongo(error_msg)
                raise Exception(error_msg)
            
            # DART 메인 페이지 이동
            if not await self._navigate_to_dart_main():
                error_msg = "DART 메인 페이지 로드 실패"
                await self.save_failed_status_to_mongo(error_msg)
                raise Exception(error_msg)
            
            # 기업명 검색
            if not await self._search_corp_name(self.corp_name):
                error_msg = "기업명 검색 실패"
                await self.save_failed_status_to_mongo(error_msg)
                raise Exception(error_msg)
            
            # 검색 결과에서 기업 선택
            await self._handle_company_selection()
            
            # 검색 조건 설정
            await self._set_search_condition()
            
            # 재무제표 목록 조회
            public_year_list, fs_url_list = await self.get_fs_list()
            
            if len(fs_url_list) == 0:
                error_msg = "사업보고서가 조회되지 않습니다"
                logger.warning(f"{self.corp_name}의 사업보고서가 조회되지 않습니다.")
                await self.save_failed_status_to_mongo(error_msg)
                return False, error_msg, [], []
            
            # 성공적으로 조회된 경우 MongoDB에 저장
            await self.save_company_info_to_mongo(public_year_list, fs_url_list, industry_levels)
            
            logger.info("기업 검색 및 재무제표 목록 조회 완료")
            return True, "성공", public_year_list, fs_url_list
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"초기화 및 검색 중 오류: {error_msg}")
            await self.save_failed_status_to_mongo(error_msg)
            await self._take_screenshot("99_error_state")
            return False, error_msg, [], []
        
        finally:
            # 브라우저 정리
            await self._cleanup_browser()