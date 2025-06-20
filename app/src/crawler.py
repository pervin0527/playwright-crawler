import re
import asyncio
import pandas as pd

from typing import Optional
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright, Page, Browser, BrowserContext, ElementHandle

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
    

    async def search_by_corp_name(self, company_name: str, stock_code: str):
        logger.info(f"[search_by_corp_name] 기업 검색 시작: {company_name}")

        search_input = self.page.locator('#textCrpNm2')
        await search_input.fill(company_name)
        await search_input.press('Enter')

        await asyncio.sleep(1)
        await self.page.wait_for_load_state('networkidle')
        await self.page.screenshot(path=f'/playwright-crawler/screenshots/01_query_input.png')

        ## 기업 검색 결과가 여러 개인 경우 팝업 창이 발생하므로 처리
        await self.search_pop_closing(company_name, stock_code)

        await self.page.locator('#date7').click() ## 기간 선택
        await self.page.locator('#li_01 > label').click() ## 정기공시 클릭
        await asyncio.sleep(1)

        await self.page.locator('#divPublicTypeDetail_01 > ul > li:nth-child(1) > span > label').click() ## 사업보고서 클릭
        await self.page.locator('#searchForm > div.subSearchWrap > div.btnArea > a.btnSearch').click()

        ## 1초 대기
        await asyncio.sleep(1)
        await self.page.wait_for_load_state('networkidle')
        await self.page.screenshot(path=f'/playwright-crawler/screenshots/02_set_option.png')

        logger.info(f"[search_by_corp_name] 기업 검색 완료: {company_name}")
        return True


    async def search_pop_closing(self, target_name:str, target_stock_code:str):
        logger.info(f"[search_pop_closing] 기업 명단 팝업 창 존재 여부 확인 시작")
        win_corp_info = self.page.locator('//*[@id="winCorpInfo"]')
        win_corp_info_style = await win_corp_info.get_attribute('style')
        logger.info(f"[search_pop_closing] 팝업창 스타일 속성: '{win_corp_info_style}'")

        # 팝업창 상태 판단 로직 수정
        # - style 속성이 None이거나 빈 문자열 → 팝업창 열림
        # - style 속성에 'display: none;' 포함 → 팝업창 닫힘
        popup_status = False
        if win_corp_info_style is None or win_corp_info_style == '':
            logger.info(f"[search_pop_closing] 기업 명단 팝업 창 열림 상태 (style 속성 없음)")
            popup_status = True
        elif 'display: none;' not in win_corp_info_style:
            logger.info(f"[search_pop_closing] 기업 명단 팝업 창 열림 상태 (display: none 없음)")
            popup_status = True
        else:
            logger.info(f"[search_pop_closing] 기업 명단 팝업 창 닫힌 상태")

        if popup_status:
            tbody = win_corp_info.locator('#corpListContents > div.tbLWrap > div.tbLInner > table > tbody')
            trs = tbody.locator('tr')
            tr_count = await trs.count()

            tr_list = await trs.all()
            for tr in tr_list:
                tds = tr.locator('td')
                
                ## 1번째 td 요소의 텍스트 추출 (기업명)
                td_text = await tds.nth(1).text_content()
                td_text = td_text[1:]

                ## 3번째 td 요소의 텍스트 추출 (종목코드)
                td_stock_code = await tds.nth(3).text_content()

                logger.info(f"[search_pop_closing] 팝업창 기업 정보: {td_text}, 종목코드: {td_stock_code}")
                
                if td_text == target_name and td_stock_code == target_stock_code:
                    logger.info(f"[search_pop_closing] 검색 조건과 일치하는 기업 발견: {td_text}, {td_stock_code}")
                    td_check = tds.nth(0)
                    await td_check.click()
                    await self.page.screenshot(path=f'/playwright-crawler/screenshots/03_select_company.png')
                    
                    # 팝업창 확인 버튼 클릭
                    confirm_btn = self.page.locator('#winCorpInfo > div.searchPop.wrapM > div.contWrap > div.btnArea > a.btnSB')
                    logger.info(f"[search_pop_closing] 팝업창 확인 버튼 존재 여부: {await confirm_btn.count()}")
                    if await confirm_btn.count() > 0:
                        await confirm_btn.click()
                        logger.info(f"[search_pop_closing] 팝업창 확인 버튼 클릭 완료")
                        await asyncio.sleep(1)  # 팝업창이 닫힐 때까지 대기
                    
                    break
            return True
        else:
            return False


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
    

    async def valid_standard_nb_table(self, table: ElementHandle):
        """nb 테이블의 표준 양식 검증"""
        is_standard_table = False
        
        try:
            trs = table.locator('tr')
            tr_count = await trs.count()
            
            # nb 테이블은 최소 5개 행이 있어야 함 (제목, 3개 연도행, 단위행)
            if tr_count < 5:
                # logger.info(f"[valid_standard_nb_table] 행 수 부족: {tr_count}")
                return False
            
            # 1, 2, 3번째 행(인덱스 1, 2, 3)에서 연도 정보 확인
            years_found = []
            for i in range(1, 4):
                tr = trs.nth(i)
                tds = tr.locator('td')
                td_count = await tds.count()
                
                if td_count >= 1:  # 최소 1개의 td가 있어야 함
                    td = tds.first
                    td_text = await td.text_content()
                    if td_text and re.match(r'제\s*\d+\s*기', td_text.strip()):
                        years_found.append(td_text.strip())
                        # logger.info(f"[valid_standard_nb_table] {i}번째 행에서 연도 발견: {td_text.strip()}")
            
            # 3개의 연도가 모두 '제 OO 기' 형태인지 확인
            if len(years_found) == 3:
                is_standard_table = True
                
        except Exception as e:
            logger.warning(f"[valid_standard_nb_table] 검증 중 오류: {str(e)}")
            
        return is_standard_table

    async def valid_standard_data_table(self, table: ElementHandle):
        """데이터 테이블(border=1)의 표준 양식 검증"""
        is_standard_table = False
        
        # 헤더 행 확인
        header_row = table.locator('thead tr').first
        if await header_row.count() > 0:
            header_cells = header_row.locator('th')
            
            is_standard_table = True
            header_count = await header_cells.count()
            logger.info(f"[valid_standard_data_table] 헤더 열 수: {header_count}")

            if header_count != 4:
                is_standard_table = False
            
            header_list = await header_cells.all()
            for j, header in enumerate(header_list):
                header_text = await header.text_content()
                if j == 0 and header_text.strip() != '':
                    is_standard_table = False
                
                if 1 < j < 4 and header_text.strip() != '':
                    # '제 OO 기' 형태가 아니라면 표준 양식이 아님
                    if not re.match(r'제\s*\d+\s*기', header_text.strip()):
                        is_standard_table = False
                        break
                logger.info(f"[valid_standard_data_table] 헤더 {j+1}: {header_text.strip() if header_text else ''}")
        else:
            logger.info(f"[valid_standard_data_table] 헤더 행을 찾을 수 없음 - 비표준 테이블로 처리")

        return is_standard_table
    

    async def search_right_panel(self):
        logger.info(f"[search_right_panel] 우측 패널 검색 시작")

        await self.page.wait_for_selector('#ifrm', timeout=30000) ## iframe이 로드될 때까지 대기
        await asyncio.sleep(1) ## iframe 내부 콘텐츠 로드 대기

        current_year = self.page.url
        current_year = current_year.split('=')[-1][:4]
        
        # URL에서 rcept_no 추출
        current_rcept_no = self.page.url.split('=')[-1]
        
        try:
            iframe = self.page.frame_locator('#ifrm') ## iframe 내부에 접근
            await iframe.locator('body').wait_for(timeout=15000) ## iframe 내부 콘텐츠 로드 대기
            
            tables = iframe.locator('table') ## iframe 내부에 속한 모든 table 요소
            table_count = await tables.count() ## table 요소의 개수
            logger.info(f"[search_right_panel] 총 {table_count}개 테이블 발견")
            
            if table_count > 0:
                tables_list = await tables.all()

                dataset = []
                check_set = set()
                for i, table in enumerate(tables_list):
                    try:
                        table_class = await table.get_attribute("class")
                        table_border = await table.get_attribute("border")
                        
                        ## nb 테이블 처리
                        if table_class == "nb":
                            # 각 테이블마다 새로운 template 생성
                            template = {
                                "corp_name": getattr(self, 'company_name', ''),
                                "stock_code": getattr(self, 'stock_code', ''),
                                "corp_code": getattr(self, 'corp_code', ''),
                                "bsns_year": current_year,
                                "rcept_no": current_rcept_no,
                                "corp_type_value": getattr(self, 'corp_type_value', ''),
                                "corp_type_name": getattr(self, 'corp_type_name', ''),
                                "sj_div": "",
                                "unit": "",
                                "data": []
                            }
                            
                            # 표준 양식 검증
                            is_standard = await self.valid_standard_nb_table(table)
                            logger.info(f"[search_right_panel] {i}번째 nb 테이블 표준 양식 여부: {is_standard}")
                            
                            if not is_standard:
                                logger.warning(f"[search_right_panel] 표준 양식이 아닌 nb 테이블 발견, 스킵합니다.")
                                continue

                            else:
                                # 표준 양식 nb 테이블 처리
                                years = []
                                trs = table.locator('tr')

                                nb_title = await trs.nth(0).text_content()
                                nb_title = clean_paragraph_text(nb_title)

                                if nb_title not in self.TARGET_SJ_LIST:
                                    continue
                                
                                # sj_div를 Selenium과 동일한 형식으로 변환
                                sj_div = nb_title
                                fs_div = ""
                                if "연결" in sj_div:
                                    fs_div = "CFS"
                                else:
                                    fs_div = "OFS"

                                if "재무상태표" in sj_div:
                                    sj_div = "BS"
                                elif "포괄손익계산서" in sj_div:
                                    sj_div = "CIS"
                                elif "손익계산서" in sj_div:
                                    sj_div = "IS"

                                template["sj_div"] = f"{fs_div}_{sj_div}"
                                logger.info(f"[search_right_panel] {i}번째 표준 nb 테이블 제목(sj_div): {template['sj_div']}")

                                for j in range(1, 4):
                                    tr = trs.nth(j)
                                    tds = tr.locator('td')
                                    tds_list = await tds.all()
                                    for td in tds_list:
                                        td_text = await td.text_content()
                                        year = extract_year_from_report_title(td_text)
                                        years.append(year)
                                    
                                logger.info(f"[search_right_panel] {i}번째 표준 nb 테이블 추출 결과 (years): {years}")

                                unit = await trs.nth(4).text_content()
                                unit = re.search(r'\(\s*단위\s*:\s*([^)]+)\)', unit)
                                if unit:
                                    unit = unit.group(1).strip()
                                
                                template["unit"] = unit
                                logger.info(f"[search_right_panel] {i}번째 표준 nb 테이블 단위: {unit}")

                                dataset.append(template)
                                logger.info(f"[search_right_panel] {i}번째 표준 nb 테이블 추출 결과: {template}")
                                

                        elif table_border == "1":
                            # 표준 양식 검증
                            is_standard = await self.valid_standard_data_table(table)
                            logger.info(f"[search_right_panel] {i}번째 데이터 테이블 표준 양식 여부: {is_standard}")
                            
                            account_data = []
                            if not is_standard:
                                raise Exception("표준 양식이 아닌 데이터 테이블 발견")
                                # TODO: 표준 양식 데이터 테이블에 대한 실제 데이터 추출 로직 구현

                            else:
                                logger.info(f"[search_right_panel] {i}번째 표준 데이터 테이블 처리 시작")
                                tbody_rows = table.locator('tbody tr')
                                tbody_count = await tbody_rows.count()
                                logger.info(f"[search_right_panel] {i}번째 표준 데이터 테이블 행 수: {tbody_count}")

                                tbody_list = await tbody_rows.all()
                                years = [str(int(current_year) - 1), str(int(current_year) - 2), str(int(current_year) - 3)]

                                # 계층 구조 추적을 위한 변수
                                current_accounts_by_level = {}
                                ord_value = 1
                                account_data = []

                                for j, row in enumerate(tbody_list):
                                    tds = row.locator('td')
                                    tds_list = await tds.all()
                                    
                                    raw_account_name = ""
                                    account_name = ""
                                    amounts = []
                                    account_level = 0
                                    ancestors = []
                                    
                                    for k, td in enumerate(tds_list):
                                        if k == 0:
                                            # 첫 번째 열에서 계정명 추출
                                            raw_account_name = await td.text_content()
                                            if raw_account_name:
                                                # 유니코드 공백 처리 (\u3000은 전각 공백)
                                                raw_account_name = raw_account_name.replace('\u3000', ' ')
                                                
                                                # 계층 구조 파악을 위해 계정명 앞의 공백 개수 확인
                                                leading_spaces = len(raw_account_name) - len(raw_account_name.lstrip())
                                                account_level = leading_spaces
                                                
                                                account_name = clean_account_name(raw_account_name)
                                                
                                                # 현재 레벨의 계정 저장
                                                current_accounts_by_level[account_level] = account_name
                                                
                                                # 상위 레벨의 계정들을 ancestors로 수집
                                                ancestors = []
                                                for level in range(account_level):
                                                    if level in current_accounts_by_level:
                                                        ancestors.append(current_accounts_by_level[level])
                                                        
                                        elif k >= 1 and k <= 3:
                                            # 2~4번째 열에서 금액 데이터 추출 (3개년 데이터)
                                            td_text = await td.text_content()
                                            if td_text:
                                                year = years[k-1] if k-1 < len(years) else ""
                                                amount = td_text.strip() if td_text.strip() else "0"
                                                amounts.append({year: amount})
                                    
                                    # 계정명이 "과목"인 경우 헤더 행이므로 스킵
                                    if account_name == "과목":
                                        continue
                                    
                                    # 계정 데이터 추가
                                    if account_name:
                                        account_data.append({
                                            "ord_value": ord_value,
                                            "raw_account_name": raw_account_name,
                                            "account_name": account_name,
                                            "amounts": amounts,
                                            "account_level": account_level,
                                            "ancestors": ancestors
                                        })
                                        ord_value += 1
                                        logger.info(f"[search_right_panel] 계정 추가: {account_name} (레벨: {account_level}, 상위계정: {ancestors})")
                                        logger.info(f"[search_right_panel] {amounts}")
                                
                                # 데이터를 dataset에 추가 (기존 template 구조와 병합)
                                if len(dataset) > 0:
                                    dataset[-1]["data"] = account_data
                                    logger.info(f"[search_right_panel] {i}번째 표준 데이터 테이블 처리 완료: {len(account_data)}개 계정")

                            
                    except Exception as e:
                        logger.warning(f"[search_right_panel] 테이블 {i+1} 처리 중 오류: {str(e)}")
                        continue
                
                logger.info(f"[search_right_panel] 총 {len(dataset)}개 재무제표 데이터 수집 완료")
                return dataset
            else:
                logger.warning(f"[search_right_panel] 테이블을 찾을 수 없습니다")
                return []
                
        except Exception as e:
            logger.error(f"[search_right_panel] iframe 접근 실패: {str(e)}")
            return []
    

    async def search_left_panel_tree(self):
        logger.info(f"[search_left_panel_tree] 좌측 트리 검색 시작")
        tree = self.page.locator('#listTree > ul')
        level1_nodes = tree.locator('.jstree-open')
        num_level1_nodes = await level1_nodes.count()
        logger.info(f"[search_left_panel_tree] 좌측 트리 검색 완료: {num_level1_nodes}개")

        # 수집된 데이터를 누적할 리스트
        collected_datasets = []

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
            return []
        
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
                # leaf 노드인 경우 - 연결재무제표나 재무제표 같은 상위 노드는 스킵하고 하위 노드를 찾아야 함
                logger.info(f"[search_left_panel_tree] level2 '{lv2_title}' - leaf 노드이지만 상위 카테고리이므로 스킵")
                # 연결재무제표나 재무제표 같은 상위 카테고리는 하위 노드가 있어야 하는데 leaf로 표시되는 경우가 있음
                # 이런 경우 클릭해서 하위 노드가 펼쳐지는지 확인
                if clean_lv2_title in ['연결재무제표', '재무제표']:
                    logger.info(f"[search_left_panel_tree] '{lv2_title}' 클릭하여 하위 노드 확인")
                    await lv2_node.locator('.jstree-anchor').first.click()
                    await asyncio.sleep(2)  # 노드가 펼쳐질 시간을 충분히 줌
                    await self.page.wait_for_load_state('networkidle')
                    
                    # 클릭 후 노드 상태 재확인
                    lv2_class_after = await lv2_node.get_attribute('class')
                    is_open_after = 'jstree-open' in lv2_class_after if lv2_class_after else False
                    
                    if is_open_after:
                        logger.info(f"[search_left_panel_tree] '{lv2_title}' 클릭 후 하위 노드가 펼쳐짐")
                        # 하위 노드들 처리
                        lv2_childrens = lv2_node.locator('.jstree-children')
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

                                dataset = await self.search_right_panel()
                                if dataset:
                                    collected_datasets.extend(dataset)
                                    logger.info(f"[search_left_panel_tree] '{lv3_title}'에서 {len(dataset)}개 데이터 수집")
                    else:
                        logger.warning(f"[search_left_panel_tree] '{lv2_title}' 클릭했지만 하위 노드가 펼쳐지지 않음")

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

                        dataset = await self.search_right_panel()
                        if dataset:
                            collected_datasets.extend(dataset)
                            logger.info(f"[search_left_panel_tree] '{lv3_title}'에서 {len(dataset)}개 데이터 수집")
                
        logger.info(f"[search_left_panel_tree] 총 {len(collected_datasets)}개 재무제표 데이터 수집 완료")
        return collected_datasets

    async def collect_financial_statements(self, company_name: str, corp_type_value: str):
        logger.info(f"[collect_financial_statements] 재무제표 수집 시작: {company_name}")

        search_result = search_company(corp_name=company_name, corp_type_value=corp_type_value)
        stock_code = search_result['stock_code']
        corp_code = search_result['corp_code']
        level1 = search_result['level1']
        level2 = search_result['level2']
        level3 = search_result['level3']
        level4 = search_result['level4']
        level5 = search_result['level5']
        
        # 기업 정보를 인스턴스 변수로 저장
        self.company_name = company_name
        self.stock_code = stock_code
        self.corp_code = corp_code
        self.corp_type_value = corp_type_value
        
        # 법인 유형 코드 매핑
        corp_type_map = {
            "all": "전체",
            "P": "유가증권시장",
            "A": "코스닥시장",
            "N": "코넥스시장",
            "E": "기타법인"
        }
        self.corp_type_name = corp_type_map.get(corp_type_value, "알 수 없음")
        
        await self.init_browser()
        await self.search_by_corp_name(company_name, stock_code)
        report_list = await self.collect_report_list()
        logger.info(f"[collect_financial_statements] 총 {len(report_list)}개 보고서 정보 수집 완료")

        total_dataset = []                
        for idx, report in enumerate(report_list):
            logger.info(f"[collect_financial_statements] {idx+1}번째 보고서 수집 시작")
            logger.info(f"[collect_financial_statements] 회사명: {report['company_name']}, 보고서명: {report['report_name']}, 발행일: {report['publish_date']}, 보고서 URL: {report['report_url']}")

            await self.page.goto(report['report_url'], wait_until='networkidle', timeout=60000)
            await self.page.screenshot(path=f'/playwright-crawler/screenshots/03_report_url.png')

            dataset = await self.search_left_panel_tree()
            total_dataset.extend(dataset if dataset else [])

        return total_dataset

