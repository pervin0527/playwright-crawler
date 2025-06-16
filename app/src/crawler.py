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
        
        table_rows = self.page.locator('#tbody tr') ## table에 속한 모든 tr 요소
        row_count = await table_rows.count()
        logger.info(f"[collect_report_list] 보고서 목록 수집 완료: {row_count}개")

        reports = []
        all_rows = await table_rows.all() ## 모든 tr 요소를 ElementHandle 리스트로 변환
        for i, row in enumerate(all_rows):
            try:
                tds = row.locator('td') ## 각 tr 요소에 속한 모든 td 요소
                index_text = await tds.nth(0).text_content() ## 1번째 td 요소의 텍스트 추출
                
                company_name_raw = await tds.nth(1).text_content() ## 2번째 td 요소의 텍스트 추출
                company_name = company_name_raw.strip() if company_name_raw else ''
                if company_name.startswith('유'): ## 회사명이 '유'로 시작하는 경우
                    company_name = re.sub(r'^유\s+', '', company_name) ## '유' 이후의 공백과 탭을 제거
                    company_name = company_name.strip() ## 공백 제거
                
                report_td = tds.nth(2) ## 3번째 td 요소
                report_link = report_td.locator('a') ## 3번째 td 요소에 속한 a 요소
                report_title_raw = await report_link.text_content() ## a 요소의 텍스트 추출
                report_title = report_title_raw.strip() if report_title_raw else '' ## 공백 제거
                report_url = await report_link.get_attribute('href') ## a 요소의 href 속성 값 추출
                
                report_name = ''
                publish_date = ''
                if report_title: ## 보고서 제목이 있는 경우
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
                logger.info(f"[collect_report_list] 행 {i+1}: {report_data['company_name']} - {report_data['publish_date']} - {report_data['report_url']}")
                
            except Exception as e:
                logger.error(f"[collect_report_list] 행 {i+1} 처리 중 오류: {str(e)}")
                continue
        
        logger.info(f"[collect_report_list] 총 {len(reports)}개 보고서 정보 수집 완료")
        return reports
    

    async def search_right_panel(self):
        logger.info(f"[search_right_panel] 우측 패널 검색 시작")
        
        # iframe이 로드될 때까지 대기
        await self.page.wait_for_selector('#ifrm', timeout=30000)
        
        # iframe 내부 콘텐츠 로드 대기
        await asyncio.sleep(3)
        
        try:
            # iframe 내부에 접근
            iframe = self.page.frame_locator('#ifrm')
            await iframe.locator('body').wait_for(timeout=15000)
            
            # 재무제표 테이블 찾기
            tables = iframe.locator('table')
            table_count = await tables.count()
            logger.info(f"[search_right_panel] 총 {table_count}개 테이블 발견")
            
            if table_count > 0:
                tables_list = await tables.all()
                
                for i, table in enumerate(tables_list):
                    try:
                        # 테이블 클래스 확인
                        table_class = await table.get_attribute("class")
                        table_border = await table.get_attribute("border")
                        
                        # 제목 테이블 (class="nb") vs 데이터 테이블 (border="1") 구분
                        if table_class == "nb":
                            # 제목 테이블 처리
                            table_title = await table.text_content()
                            clean_title = table_title.strip().replace('\n', ' ').replace('\t', ' ')
                            logger.info(f"[search_right_panel] 제목 테이블 {i+1}: {clean_title}")
                            
                        elif table_border == "1":
                            # 데이터 테이블 처리
                            logger.info(f"[search_right_panel] 데이터 테이블 {i+1} 발견")
                            
                            # 테이블 행 수 확인
                            rows = table.locator('tr')
                            row_count = await rows.count()
                            logger.info(f"[search_right_panel] 데이터 테이블 {i+1} 행 수: {row_count}")
                            
                            # 헤더 행 확인
                            header_row = table.locator('thead tr').first
                            if await header_row.count() > 0:
                                header_cells = header_row.locator('th')
                                header_count = await header_cells.count()
                                logger.info(f"[search_right_panel] 헤더 열 수: {header_count}")
                                
                                # 헤더 내용 출력
                                header_list = await header_cells.all()
                                for j, header in enumerate(header_list):
                                    header_text = await header.text_content()
                                    logger.info(f"[search_right_panel] 헤더 {j+1}: {header_text.strip() if header_text else ''}")
                            
                            # 처음 몇 행의 데이터 샘플 출력
                            tbody_rows = table.locator('tbody tr')
                            tbody_count = await tbody_rows.count()
                            sample_count = min(5, tbody_count)  # 최대 5행만 샘플로 출력
                            
                            if sample_count > 0:
                                logger.info(f"[search_right_panel] 데이터 샘플 (처음 {sample_count}행):")
                                tbody_list = await tbody_rows.all()
                                
                                for k in range(sample_count):
                                    row = tbody_list[k]
                                    cells = row.locator('td')
                                    cell_count = await cells.count()
                                    
                                    cell_texts = []
                                    cell_list = await cells.all()
                                    for cell in cell_list:
                                        cell_text = await cell.text_content()
                                        cell_texts.append(cell_text.strip() if cell_text else '')
                                    
                                    logger.info(f"[search_right_panel] 행 {k+1}: {cell_texts}")
                        
                        else:
                            logger.info(f"[search_right_panel] 기타 테이블 {i+1}: 클래스={table_class}, border={table_border}")
                            
                    except Exception as e:
                        logger.warning(f"[search_right_panel] 테이블 {i+1} 처리 중 오류: {str(e)}")
            else:
                logger.warning(f"[search_right_panel] 테이블을 찾을 수 없습니다")
                
        except Exception as e:
            logger.error(f"[search_right_panel] iframe 접근 실패: {str(e)}")
            
            # 대안: content_frame 방법 시도
            try:
                iframe_element = await self.page.wait_for_selector('#ifrm', timeout=10000)
                frame = await iframe_element.content_frame()
                
                if frame:
                    await frame.wait_for_load_state('networkidle', timeout=15000)
                    
                    tables = frame.locator('table')
                    table_count = await tables.count()
                    logger.info(f"[search_right_panel] content_frame으로 {table_count}개 테이블 발견")
                    
                    # 동일한 테이블 처리 로직 적용...
                    
                else:
                    logger.error(f"[search_right_panel] content_frame을 가져올 수 없습니다")
                    
            except Exception as frame_e:
                logger.error(f"[search_right_panel] content_frame 방법도 실패: {str(frame_e)}")

        return True
    

    async def search_left_panel_tree(self):
        logger.info(f"[search_left_panel_tree] 좌측 트리 검색 시작")
        tree = self.page.locator('#listTree > ul')
        level1_nodes = tree.locator('.jstree-open')
        num_level1_nodes = await level1_nodes.count()
        logger.info(f"[search_left_panel_tree] 좌측 트리 검색 완료: {num_level1_nodes}개")

        target_lv1_idx = None
        level1_nodes_list = await level1_nodes.all()
        for lv1_idx, level1_node in enumerate(level1_nodes_list):
            lv1_anchor = level1_node.locator('.jstree-anchor').first
            lv1_title = await lv1_anchor.text_content()
            
            if "재무에 관한 사항" in lv1_title:
                target_lv1_idx = lv1_idx
                break

        if target_lv1_idx is None:
            logger.error(f"[search_left_panel_tree] '재무에 관한 사항' 노드를 찾을 수 없습니다.")
            return False
        
        logger.info(f"[search_left_panel_tree] '재무에 관한 사항' 노드 발견")
        target_lv1_node = level1_nodes_list[target_lv1_idx]
        await target_lv1_node.locator('.jstree-anchor').first.click()
        
        await asyncio.sleep(1)
        await self.page.wait_for_load_state('networkidle')
        await self.page.screenshot(path=f'/playwright-crawler/screenshots/04_search_left_panel_tree.png')

        ## 재무에 관한 사항 하위 노드들 탐색
        target_lv1_childrens = target_lv1_node.locator('.jstree-children') ## ul
        
        # level2 노드들을 모두 가져옴 (jstree-open, jstree-node, jstree-leaf 모두 포함)
        target_lv2_nodes = target_lv1_childrens.locator('li')
        num_target_lv2_nodes = await target_lv2_nodes.count()
        logger.info(f"[search_left_panel_tree] level1 '재무에 관한 사항'의 자식 노드 수: {num_target_lv2_nodes}")

        target_lv2_nodes_list = await target_lv2_nodes.all()
        for lv2_idx, lv2_node in enumerate(target_lv2_nodes_list):
            lv2_anchor = lv2_node.locator('.jstree-anchor').first
            lv2_title = await lv2_anchor.text_content()

            clean_lv2_title = lv2_title.split(".")[-1].strip()
            if clean_lv2_title not in ['연결재무제표', '재무제표']:
                continue
                
            logger.info(f"[search_left_panel_tree] level2 '{lv2_title}' 노드 발견")

            # 노드 클래스 확인
            lv2_class = await lv2_node.get_attribute('class')
            is_leaf = 'jstree-leaf' in lv2_class if lv2_class else False
            is_open = 'jstree-open' in lv2_class if lv2_class else False
            
            if is_leaf:
                # leaf 노드인 경우 바로 처리
                logger.info(f"[search_left_panel_tree] level2 '{lv2_title}' - leaf 노드로 바로 처리")
                if any(target_sj in lv2_title for target_sj in self.TARGET_SJ_LIST):
                    logger.info(f"[search_left_panel_tree] 타겟 노드 발견: {lv2_title}")
                    await lv2_node.locator('.jstree-anchor').first.click()
                    await asyncio.sleep(1)
                    await self.page.wait_for_load_state('networkidle')
                    await self.page.screenshot(path=f'/playwright-crawler/screenshots/04_search_left_panel_{lv2_title}.png')

                    await self.search_right_panel()

            else:
                # open 노드인 경우 하위 노드들 탐색
                logger.info(f"[search_left_panel_tree] level2 '{lv2_title}' - 하위 노드 탐색")
                lv2_childrens = lv2_node.locator('.jstree-children') ## ul
                lv3_nodes = lv2_childrens.locator('li')
                num_lv3_nodes = await lv3_nodes.count()
                logger.info(f"[search_left_panel_tree] level2 '{lv2_title}' 노드의 자식 노드 수: {num_lv3_nodes}")

                lv3_nodes_list = await lv3_nodes.all()
                for lv3_idx, lv3_node in enumerate(lv3_nodes_list):
                    lv3_anchor = lv3_node.locator('.jstree-anchor').first
                    lv3_title = await lv3_anchor.text_content()
                    logger.info(f"[search_left_panel_tree] level3 '{lv3_title}' 노드 발견")
                    
                    # 하위 노드가 타겟 리스트에 포함되는지 확인
                    if any(target_sj in lv3_title for target_sj in self.TARGET_SJ_LIST):
                        logger.info(f"[search_left_panel_tree] 타겟 노드 발견: {lv3_title}")
                        await lv3_node.locator('.jstree-anchor').first.click()
                        await asyncio.sleep(1)
                        await self.page.wait_for_load_state('networkidle')
                        await self.page.screenshot(path=f'/playwright-crawler/screenshots/04_search_left_panel_{lv3_title}.png')

                        await self.search_right_panel()
                
        return True

    async def collect_financial_statements(self, company_name: str):
        logger.info(f"[collect_financial_statements] 재무제표 수집 시작: {company_name}")
        
        await self.init_browser()
        await self.search_company(company_name)
        report_list = await self.collect_report_list()
        
        logger.info(f"[collect_financial_statements] 총 {len(report_list)}개 보고서 정보 수집 완료")
        for idx, report in enumerate(report_list):
            logger.info(f"[collect_financial_statements] {idx+1}번째 보고서 수집 시작")
            logger.info(f"[collect_financial_statements] 회사명: {report['company_name']}, 보고서명: {report['report_name']}, 발행일: {report['publish_date']}, 보고서 URL: {report['report_url']}")

            await self.page.goto(report['report_url'], wait_until='networkidle', timeout=60000)
            await self.page.screenshot(path=f'/playwright-crawler/screenshots/03_report_url.png')

            await self.search_left_panel_tree()
            break