import os
import re
import gc
import time
import psutil
import pandas as pd

from typing import Optional
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC

from app.src.corp_code import search_company
from app.utils.time import get_current_korea_time
from app.utils.data import clean_account_name, clean_paragraph_text, extract_year_from_report_title, extract_years_and_amounts

from app.utils.logging import logger


class FinancialStatementCrawler:
    INIT_URL = "https://dart.fss.or.kr/main.do"
    TARGET_SJ_LIST = [
        "연결재무제표", "재무제표",
        "연결재무상태표", "연결손익계산서", "연결포괄손익계산서", 
        "재무상태표", "손익계산서", "포괄손익계산서",
    ]
    
    def __init__(self, corp_name:str, stock_code:str, mongo_client=None, corp_code:Optional[str] = None, corp_type_value:str = "all", retry_count:int = 3):
        self.corp_name = corp_name
        self.stock_code = stock_code
        self.corp_code = corp_code if corp_code else None
        self.corp_type_value = corp_type_value
        self.retry_count = retry_count
        self.timeout = 5  # 타임아웃을 10초에서 5초로 줄임
        
        # 법인 유형 코드 매핑
        self.corp_type_map = {
            "all": "전체",
            "P": "유가증권시장",
            "A": "코스닥시장",
            "N": "코넥스시장",
            "E": "기타법인"
        }

        # 외부에서 제공된 MongoDB 클라이언트 처리
        if mongo_client:
            # AsyncIOMotorClient인 경우 동기식 MongoClient로 변환
            if isinstance(mongo_client, AsyncIOMotorClient):
                logger.info("AsyncIOMotorClient를 동기식 MongoClient로 변환합니다")
                # MongoDB 환경 변수에서 연결 문자열 가져오기
                mongodb_url = os.getenv("MONGODB_URL")
                if not mongodb_url:
                    # 환경 변수가 없는 경우 기본 로컬 MongoDB 연결 시도
                    mongodb_url = "mongodb://localhost:27017"
                    logger.warning(f"MONGODB_URL 환경 변수가 없어 기본값 사용: {mongodb_url}")
                else:
                    logger.info(f"환경 변수에서 MongoDB URL 사용: {mongodb_url}")
                self.mongo_client = MongoClient(mongodb_url)
            else:
                self.mongo_client = mongo_client
        else:
            # 기존 방식으로 MongoDB 클라이언트 생성 (백업용)
            mongodb_url = os.getenv("MONGODB_URL")
            logger.info(f"새 MongoClient 생성: {mongodb_url}")
            self.mongo_client = MongoClient(mongodb_url)

        # 초기 드라이버는 None으로 설정하고 실제 사용 시점에 생성
        self.driver = None


    def _search_corp_name(self, corp_name:str):
        layout_notice = self.driver.find_element(By.CLASS_NAME, "layoutNotice")
        main_pagebg = layout_notice.find_element(By.CLASS_NAME, "mainPageBg")
        main_search_wrap = main_pagebg.find_element(By.CLASS_NAME, "mainSearchWrap")
        
        main_search = main_search_wrap.find_element(By.ID, "mainSearch")
        search_wrap = main_search.find_element(By.CLASS_NAME, "searchWrap")
        search_form2 = search_wrap.find_element(By.ID, "searchForm2")
        search = search_form2.find_element(By.CLASS_NAME, "search")
        auto_wrap = search.find_element(By.CLASS_NAME, "autoWrap")
        search_area = auto_wrap.find_element(By.ID, "searchArea_crp2")
        
        text_crp_nm = search_area.find_element(By.ID, "textCrpNm2")
        text_crp_nm.send_keys(corp_name)
        text_crp_nm.send_keys(Keys.RETURN)


    def _set_search_condition(self):
        try:
            sub_page_bg = self.driver.find_element(By.CLASS_NAME, "subPageBg")
            container = sub_page_bg.find_element(By.ID, "container")
            contents_wrap = container.find_element(By.ID, "contentsWrap")
            contents = contents_wrap.find_element(By.ID, "contents")
            page = contents.find_element(By.ID, "page")

            search_form = page.find_element(By.ID, "searchForm")
            sub_search_wrap = search_form.find_element(By.CLASS_NAME, "subSearchWrap")
            sub_search = sub_search_wrap.find_element(By.CLASS_NAME, "subSearch")

            ul = sub_search.find_element(By.TAG_NAME, "ul")
            lis = ul.find_elements(By.TAG_NAME, "li")
            period_wrap = lis[2]
            rwrap = period_wrap.find_element(By.CLASS_NAME, "rWrap")
            date_select = rwrap.find_element(By.CLASS_NAME, "dateSelect")
            date_btns = date_select.find_elements(By.CLASS_NAME, "btnDate")

            # 클릭 가능해질 때까지 짧은 시간 대기
            try:
                WebDriverWait(self.driver, 2).until(EC.element_to_be_clickable((By.ID, "date7")))
                date_btns[-1].click()
            except TimeoutException:
                logger.warning("버튼 클릭 대기 중 타임아웃, 직접 클릭 시도")
                date_btns[-1].click()
            
            disclosure_type = lis[3]
            sub_check = disclosure_type.find_element(By.ID, "subCheck")
            span = sub_check.find_element(By.TAG_NAME, "span")
            ul = span.find_element(By.TAG_NAME, "ul")
            lis = ul.find_elements(By.TAG_NAME, "li")
            lis[0].click()

            detail_check_wrap = sub_search_wrap.find_element(By.ID, "detailCheckWrap")
            detail_check = detail_check_wrap.find_element(By.CLASS_NAME, "detailCheck")
            ul = detail_check.find_element(By.TAG_NAME, "ul")
            lis = ul.find_elements(By.TAG_NAME, "li")
            span = lis[0].find_element(By.CLASS_NAME, "frmCheck")
            label = span.find_element(By.TAG_NAME, "label")
            label.click()

            btn_area = sub_search_wrap.find_element(By.CLASS_NAME, "btnArea")
            btn_search = btn_area.find_element(By.CLASS_NAME, "btnSearch")
            btn_search.click()
            time.sleep(1)
        except Exception as e:
            logger.error(f"검색 조건 설정 중 오류 발생: {e}")
            raise


    def get_fs_list(self):
        page = self.driver.find_element(By.ID, "page")
        list_contents = page.find_element(By.ID, "listContents")
        tb_list_inner = list_contents.find_element(By.CLASS_NAME, "tbListInner")
        tb_list = tb_list_inner.find_element(By.CLASS_NAME, "tbList")
        
        tbody = tb_list.find_element(By.ID, "tbody")
        trs = tbody.find_elements(By.TAG_NAME, "tr")
        logger.info(f'[get_fs_list] 사업보고서 개수 : {len(trs)}')
        
        fs_url_list = []
        public_year_list = []
        if len(trs) == 1:
            td = trs[0].find_element(By.TAG_NAME, "td")
            if "no_data" in td.get_attribute("class"):
                logger.warning(f"{self.corp_name} 기업코드 : {self.corp_code}, 종목코드 : {self.stock_code} 의 사업보고서가 조회되지 않습니다.")
                return [], []

        for tr in trs:
            tds = tr.find_elements(By.TAG_NAME, "td")
            anchor = tds[2].find_element(By.TAG_NAME, "a")
            title = anchor.text
            public_year = extract_year_from_report_title(title)
            public_year_list.append(public_year)

            url = anchor.get_attribute("href")
            fs_url_list.append(url)

        return public_year_list, fs_url_list
    

    def crawling_dataset(self, corp_name:str, stock_code:str, corp_code:str, bsns_year:str, rcept_no:str):
        try:
            ## iframe 내부 콘텐츠 로드 대기 - 짧은 대기 시간 사용
            WebDriverWait(self.driver, 2).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            body = self.driver.find_element(By.TAG_NAME, "body")

            paragraphs = body.find_elements(By.TAG_NAME, "p")
            sj_div_list = []
            for paragraph in paragraphs:
                ptext = clean_paragraph_text(paragraph.text)
                if ptext in ["재무상태표", "손익계산서", "포괄손익계산서", "연결재무상태표", "연결손익계산서", "연결포괄손익계산서"] and ptext not in sj_div_list:
                    sj_div_list.append(ptext)

            try:
                # 짧은 대기 시간으로 테이블 찾기
                WebDriverWait(self.driver, 2).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                tables = body.find_elements(By.TAG_NAME, "table")
                nb_tables = [table for table in tables if table.get_attribute("class") == "nb"]
                data_tables = [table for table in tables if table.get_attribute("border") == "1"]
            
            except TimeoutException:
                logger.error(f"right_panel에서 테이블을 찾지 못함: 타임아웃 발생.")
                nb_tables = []
                data_tables = []

            # nb_tables에서 실제 재무제표 제목이 포함된 테이블들을 찾기
            relevant_nb_tables = []
            for nb_table in nb_tables:
                try:
                    tbody = nb_table.find_element(By.TAG_NAME, "tbody")
                    trs = tbody.find_elements(By.TAG_NAME, "tr")
                    
                    # 이 테이블에서 재무제표 제목을 찾는지 확인
                    found_fs_title = False
                    for tr in trs:
                        tds = tr.find_elements(By.TAG_NAME, "td")
                        for td in tds:
                            td_text = clean_account_name(td.text)
                            if td_text in self.TARGET_SJ_LIST:
                                relevant_nb_tables.append(nb_table)
                                found_fs_title = True
                                break
                        if found_fs_title:
                            break
                except:
                    continue
            
            # 실제 재무제표 개수는 sj_div_list, relevant_nb_tables, data_tables 길이 중 최소값
            actual_fs_count = min(len(sj_div_list), len(relevant_nb_tables), len(data_tables))
            
            dataset = []
            for idx in range(actual_fs_count):
                data = {
                    "corp_name": corp_name,
                    "stock_code": stock_code,
                    "corp_code": corp_code,
                    "bsns_year": bsns_year,
                    "rcept_no": rcept_no,
                    "corp_type_value": self.corp_type_value,
                    "corp_type_name": self.corp_type_map.get(self.corp_type_value, "알 수 없음"),
                    "sj_div": "",
                    "unit": "",
                    "data": []
                }
                
                # 관련 nb_table 사용
                nb_table = relevant_nb_tables[idx]
                tbody = nb_table.find_element(By.TAG_NAME, "tbody")
                trs = tbody.find_elements(By.TAG_NAME, "tr")

                last_row = trs[-1]
                last_row_td = last_row.find_element(By.TAG_NAME, "td")
                last_row_text = last_row_td.text
                
                # 단위 추출 로직 개선 - 정규표현식 사용
                unit_match = re.search(r'\(\s*단위\s*:\s*([^)]+)\)', last_row_text)
                if unit_match:
                    unit = unit_match.group(1).strip()
                else:
                    unit = "알 수 없음"
                
                # 추출한 단위를 data에 저장
                data["unit"] = unit
                for tr in trs:
                    tds = tr.find_elements(By.TAG_NAME, "td")
                    for td in tds:
                        td_text = clean_account_name(td.text)
                        if td_text in self.TARGET_SJ_LIST:
                            sj_div = td_text

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

                            data["sj_div"] = f"{fs_div}_{sj_div}"

                if data["sj_div"] == "":
                    if idx < len(sj_div_list):
                        current_sj = sj_div_list[idx]
                        if current_sj == "재무상태표":
                            data["sj_div"] = "OFS_BS"
                        elif current_sj == "손익계산서":
                            data["sj_div"] = "OFS_IS"
                        elif current_sj == "포괄손익계산서":
                            data["sj_div"] = "OFS_CIS"
                        elif current_sj == "연결재무상태표":
                            data["sj_div"] = "CFS_BS"
                        elif current_sj == "연결손익계산서":
                            data["sj_div"] = "CFS_IS"
                        elif current_sj == "연결포괄손익계산서":
                            data["sj_div"] = "CFS_CIS"

                if data["sj_div"] == "":
                    continue

                else:
                    dataset.append(data)

            for data_idx, data in enumerate(dataset):
                data_table = data_tables[data_idx]
                tbody = data_table.find_element(By.TAG_NAME, "tbody")
                trs = tbody.find_elements(By.TAG_NAME, "tr")

                noise_col_idx = -1
                from_header = False
                try:
                    thead = data_table.find_element(By.TAG_NAME, "thead")
                    thead_trs = thead.find_elements(By.TAG_NAME, "tr")
                    
                    for thead_tr in thead_trs:
                        ths = thead_tr.find_elements(By.TAG_NAME, "th")
                        for th_idx, th in enumerate(ths):
                            th_text = clean_account_name(th.text)
                            if th_text == "주석":
                                noise_col_idx = th_idx
                                from_header = True
                                break
                except:
                    pass

                if noise_col_idx == -1 and not from_header:
                    first_row = trs[0]
                    tds = first_row.find_elements(By.TAG_NAME, "td")
                    for td_idx, td in enumerate(tds):
                        if td.text == "주석":
                            noise_col_idx = td_idx
                            break

                ord_value = 1
                current_accounts_by_level = {}  # 각 레벨별 현재 계정 저장
                for row_idx, tr in enumerate(trs):
                    raw_account_name = ""
                    amounts = []
                    curr_year = int(bsns_year)
                    account_level = 0
                    ancestors = []

                    # 주석 열이 있고 헤더에서 찾은 것이 아니면, 첫 번째 행(주석 헤더 행)만 스킵
                    if row_idx == 0 and noise_col_idx != -1 and not from_header:
                        continue

                    tds = tr.find_elements(By.TAG_NAME, "td")
                    curr_account_name = ""  # 변수 미리 초기화
                    for col_idx, td in enumerate(tds):
                        if col_idx == 0:
                            try:
                                # p 태그가 없을 수 있으므로 직접 텍스트도 추출
                                td_text = td.text.replace('\u3000', ' ')
                                try:
                                    raw_account_name = td.find_element(By.TAG_NAME, "p").get_attribute("textContent").replace('\u3000', ' ')
                                except:
                                    raw_account_name = td_text

                                                                # 계층 구조 파악을 위해 계정명 앞의 공백 개수 확인
                                leading_spaces = len(raw_account_name) - len(raw_account_name.lstrip())
                                account_level = leading_spaces
                                
                                clean_raw_account_name = clean_account_name(raw_account_name)
                                curr_account_name = clean_account_name(clean_raw_account_name)
                                
                                # 현재 레벨의 계정 저장
                                current_accounts_by_level[account_level] = curr_account_name
                                
                                # 상위 레벨의 계정들을 ancestors로 수집
                                ancestors = []
                                for level in range(account_level):
                                    if level in current_accounts_by_level:
                                        ancestors.append(current_accounts_by_level[level])

                            except Exception as e:
                                logger.error(f"raw_account_name 추출 실패 : {e}")
                                logger.error(f"td : {td.text}")
                                # 계속 진행하지 않고 다음 tr로 넘어감
                                break

                        else:
                            if col_idx == noise_col_idx:
                                continue
                            
                            account_amount = td.text
                            if account_amount == "":
                                account_amount = "0"

                            amounts.append({str(curr_year): account_amount})
                            curr_year -= 1

                    if curr_account_name == "과목":
                        continue

                    if len(amounts) > 3:
                        output = extract_years_and_amounts(amounts)
                        years = [item[0] for item in output][:3]
                        values = [item[1] for item in output]

                        if all(value == "0" for value in values):
                            values = ["0"] * 3
                        else:
                            values = [str(value) for value in values if value != "0"]
                        
                        if len(values) < 3:
                            values = values + ["0"] * (3 - len(values))

                        amounts = {year: value for year, value in zip(years, values)}
                
                    data["data"].append({
                        "ord_value": ord_value,
                        "raw_account_name": raw_account_name,
                        "account_name": curr_account_name,
                        "amounts": amounts,
                        "account_level": account_level,
                        "ancestors": ancestors  # 상위 계정 리스트
                    })
                    ord_value += 1
            
            return dataset
        
        except Exception as e:
            logger.error(f"재무제표 크롤링 중 오류 발생: {e}")
            return []


    def right_panel_slider(self, content_wrap_div:WebElement, corp_name:str, stock_code:str, corp_code:str, bsns_year:str, rcept_no:str):
        try:
            right_panel = content_wrap_div.find_element(By.ID, "right-panel")
            contents = right_panel.find_element(By.CLASS_NAME, "contents")
            view_wrap = contents.find_element(By.CLASS_NAME, "viewWrap")
            contwrap = view_wrap.find_element(By.CLASS_NAME, "contWrap")
            
            # 짧은 대기 시간으로 iframe 찾기 시도
            WebDriverWait(self.driver, 2).until(EC.presence_of_element_located((By.ID, "ifrm")))
            iframe = contwrap.find_element(By.ID, "ifrm")
            self.driver.switch_to.frame(iframe)
            
            ## 재무제표 데이터 수집
            dataset = self.crawling_dataset(corp_name, stock_code, corp_code, bsns_year, rcept_no)

            ## iframe에서 기본 컨텍스트로 다시 전환
            self.driver.switch_to.default_content()
            return dataset
        
        except TimeoutException:
            logger.error("iframe을 찾는 중 타임아웃 발생")
            return []
        
        except Exception as e:
            logger.error(f"right_panel_slider 오류: {str(e)}")
            return []


    def left_panel_slider(self, corp_name:str, stock_code:str, corp_code:str, bsns_year:str, rcept_no:str):
        wrapper = self.driver.find_element(By.CLASS_NAME, "wrapper")
        viewerPop = wrapper.find_element(By.CLASS_NAME, "viewerPop")

        contents_wrap_div = viewerPop.find_element(By.ID, "contentsWrapDiv")
        left_panel = contents_wrap_div.find_element(By.ID, "left-panel")

        left_panel_content = left_panel.find_element(By.ID, "left-panel-content")
        listTree = left_panel_content.find_element(By.ID, "listTree")

        jstree_container_ul = listTree.find_element(By.CLASS_NAME, "jstree-container-ul")  # 전체 리스트
        level1_list = jstree_container_ul.find_elements(By.CLASS_NAME, "jstree-open")  # 최상위 노드들 (레벨 1)

        level1_fs_node = None
        for level1_node in level1_list:
            anchor = level1_node.find_element(By.TAG_NAME, "a")
            anchor_text = anchor.text

            if "재무에 관한 사항" in anchor_text:
                level1_fs_node = level1_node
                break
        else:
            logger.info(f"['재무에 관한 사항']을 찾지 못했습니다. --> URL : {self.driver.current_url}")
            return []
        
        level1_jstree_children = level1_fs_node.find_element(By.CLASS_NAME, "jstree-children")  # 레벨 1 자식들 (레벨 2)
        level2_list = level1_jstree_children.find_elements(By.CLASS_NAME, "jstree-open")  # 레벨 2 노드들
        
        all_collected_data = []  # 수집된 모든 데이터를 저장할 리스트
        if len(level2_list) > 0:
            for level2_node in level2_list:
                level2_jstree_children = level2_node.find_element(By.CLASS_NAME, "jstree-children")
                level3_list = level2_jstree_children.find_elements(By.CLASS_NAME, "jstree-leaf")

                for level3_node in level3_list:
                    anchor = level3_node.find_element(By.TAG_NAME, "a")
                    sj_title = clean_account_name(anchor.text)
                    
                    if sj_title in self.TARGET_SJ_LIST:
                        anchor.click()
                        time.sleep(1)
                        dataset = self.right_panel_slider(contents_wrap_div, corp_name, stock_code, corp_code, bsns_year, rcept_no)
                        all_collected_data.extend(dataset if dataset else [])
                        
                        for data in dataset:
                            try:
                                logger.info(f"MongoDB에 저장 시도: {data['sj_div']} (기업: {corp_name}, 년도: {bsns_year})")
                                specific_collection = self.mongo_client["dart"][data["sj_div"]]
                                
                                # 기존 문서 확인
                                query = {
                                    "corp_code": data["corp_code"],
                                    "stock_code": data["stock_code"],
                                    "bsns_year": data["bsns_year"],
                                    "rcept_no": data["rcept_no"]
                                }
                                
                                existing_doc = specific_collection.find_one(query)
                                
                                if existing_doc:
                                    # 기존 문서가 있으면 업데이트
                                    data["updated_at"] = get_current_korea_time()
                                    result = specific_collection.update_one(query, {"$set": data})
                                    logger.info(f"재무제표 데이터 업데이트: {data['corp_name']}, {data['bsns_year']}, {data['sj_div']}, modified: {result.modified_count}")
                                else:
                                    # 새 문서 삽입
                                    data["created_at"] = get_current_korea_time()
                                    result = specific_collection.insert_one(data)
                                    logger.info(f"재무제표 데이터 저장: {data['corp_name']}, {data['bsns_year']}, {data['sj_div']}, ID: {result.inserted_id}")
                            except Exception as e:
                                logger.error(f"MongoDB 저장 중 오류: {e}")
                    else:
                        continue
        
        else:
            level2_list = level1_jstree_children.find_elements(By.CLASS_NAME, "jstree-node")
            if len(level2_list) > 0:
                for level2_node in level2_list:
                    try:
                        anchor = level2_node.find_element(By.TAG_NAME, "a")
                        sj_title = clean_account_name(anchor.text)

                    except Exception as e:
                        continue

                    if sj_title in self.TARGET_SJ_LIST:
                        anchor.click()
                        time.sleep(1)
                        dataset = self.right_panel_slider(contents_wrap_div, corp_name, stock_code, corp_code, bsns_year, rcept_no)
                        all_collected_data.extend(dataset if dataset else [])
                        
                        for data in dataset:
                            # print(data)
                            # 데이터베이스에 저장
                            try:
                                logger.info(f"MongoDB에 저장 시도: {data['sj_div']} (기업: {corp_name}, 년도: {bsns_year})")
                                # 재무제표 유형에 따라 알맞은 컬렉션에 저장
                                specific_collection = self.mongo_client["dart"][data["sj_div"]]
                                
                                # 기존 문서 확인
                                query = {
                                    "corp_code": data["corp_code"],
                                    "stock_code": data["stock_code"],
                                    "bsns_year": data["bsns_year"],
                                    "rcept_no": data["rcept_no"]
                                }
                                
                                existing_doc = specific_collection.find_one(query)
                                
                                if existing_doc:
                                    # 기존 문서가 있으면 업데이트
                                    data["updated_at"] = get_current_korea_time()
                                    result = specific_collection.update_one(
                                        query,
                                        {"$set": data}
                                    )
                                    logger.info(f"재무제표 데이터 업데이트: {data['corp_name']}, {data['bsns_year']}, {data['sj_div']}, modified: {result.modified_count}")
                                else:
                                    # 새 문서 삽입
                                    data["created_at"] = get_current_korea_time()
                                    result = specific_collection.insert_one(data)
                                    logger.info(f"재무제표 데이터 저장: {data['corp_name']}, {data['bsns_year']}, {data['sj_div']}, ID: {result.inserted_id}")
                            except Exception as e:
                                logger.error(f"MongoDB 저장 중 오류: {e}")
                    else:
                        # logger.info(f"sj_title : {sj_title} 대상 목록에 없음")
                        continue
            else:
                logger.info(f"['재무에 관한 사항'] 하위 항목들이 없습니다. --> URL : {self.driver.current_url}")
                return []
        
        return all_collected_data


    def _create_driver(self):
        """크롬 드라이버를 새로 생성해서 반환"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # 헤드리스 모드
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-software-rasterizer')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--js-flags=--expose-gc')
        
        service = Service("/usr/local/bin/chromedriver-linux64/chromedriver")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 명시적 타임아웃 설정
        driver.set_page_load_timeout(self.timeout)  # 페이지 로드 타임아웃
        driver.set_script_timeout(self.timeout)     # 스크립트 실행 타임아웃
        
        logger.info(f"WebDriver 생성 완료 (타임아웃: {self.timeout}초)")
        return driver

    def _clean_up_driver(self):
        """드라이버를 안전하게 종료하고 메모리를 정리"""
        if self.driver:
            try:
                # 열려있는 모든 윈도우 닫기
                self.driver.close()
                logger.info("드라이버 창 닫기 완료")
            except Exception as e:
                logger.error(f"드라이버 창 닫기 실패: {str(e)}")
        
            try:
                # 드라이버 인스턴스 종료
                self.driver.quit()
                logger.info("드라이버 인스턴스 종료 완료")
            except Exception as e:
                logger.error(f"드라이버 인스턴스 종료 실패: {str(e)}")
        
            # 드라이버 객체 참조 제거
            self.driver = None
        
            # 명시적 가비지 컬렉션 호출
            gc.collect()
        
            # 현재 프로세스의 메모리 사용량 로깅
            try:
                process = psutil.Process(os.getpid())
                memory_info = process.memory_info()
                logger.info(f"현재 메모리 사용량: {memory_info.rss / 1024 / 1024:.2f} MB")
            except Exception as e:
                logger.error(f"메모리 사용량 확인 실패: {str(e)}")

    def get_corp_fs(self):
        collected_data = []
        
        # 재무제표 유형별 수집 결과 추적
        fs_collection_results = {
            "CFS_BS": {"status": "not_found", "data_count": 0},
            "CFS_IS": {"status": "not_found", "data_count": 0},
            "CFS_CIS": {"status": "not_found", "data_count": 0},
            "OFS_BS": {"status": "not_found", "data_count": 0},
            "OFS_IS": {"status": "not_found", "data_count": 0},
            "OFS_CIS": {"status": "not_found", "data_count": 0}
        }
        
        try:
            company_info = search_company(self.corp_name, self.corp_type_value)
            
            industry_levels = {}
            if company_info:
                # company_info에서 corp_code가 None이 아닌 경우 해당 값으로 업데이트
                if company_info.get('corp_code'):
                    self.corp_code = company_info['corp_code']
                
                # stock_code가 None이고 company_info에서 값이 있는 경우 업데이트
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
            
            # 주식코드나 기업코드가 없는 경우 크롤링 중단
            if self.stock_code is None:
                logger.error(f"주식코드가 없어 크롤링을 중단합니다: {self.corp_name}")
                return False, "주식코드를 찾을 수 없습니다", [], fs_collection_results

            logger.info(f"기업명 : {self.corp_name}, 기업코드 : {self.corp_code}, 주식코드 : {self.stock_code}")
            logger.info(f"산업 분류 정보 : {industry_levels}")
            try:
                # 초기 드라이버 생성
                self.driver = self._create_driver()
                self.driver.get(self.INIT_URL)
                logger.info(f"페이지 로드 성공. 현재 URL : {self.driver.current_url}")
                
                # 기업명 검색
                self._search_corp_name(self.corp_name)
                time.sleep(1.5)
                
                ## 검색 결과로 여러 개의 회사 명단이 나온 경우
                winCorpInfo = self.driver.find_element(By.ID, "winCorpInfo")
                select_window = False
                if winCorpInfo.get_attribute('style') != "display: none;":
                    select_window = True
                
                logger.info(f"기업 선택 창 표시 여부: {select_window}")
                if select_window:
                    searchpop = self.driver.find_element(By.CLASS_NAME, "searchPop")
                    contwrap = searchpop.find_element(By.CLASS_NAME, "contWrap")
                    listcontents = contwrap.find_element(By.ID, "corpListContents")
                    tblwrap = listcontents.find_element(By.CLASS_NAME, "tbLWrap")
                    tblinner = tblwrap.find_element(By.CLASS_NAME, "tbLInner")
                    table = tblinner.find_element(By.TAG_NAME, "table")
                    tbody = table.find_element(By.TAG_NAME, "tbody")
                    trs = tbody.find_elements(By.TAG_NAME, "tr")
                    logger.info(f"검색된 기업 수: {len(trs)}")
                
                    for tr in trs:
                        tds = tr.find_elements(By.TAG_NAME, "td")                
                        td1 = tds[1]
                        candidate_corp_name = td1.text[1:]
    
                        td3 = tds[3]
                        candidate_stock_code = td3.text
                        logger.info(f"후보 기업: {candidate_corp_name}, 종목코드: {candidate_stock_code}")
    
                        if self.corp_name == candidate_corp_name and self.stock_code == candidate_stock_code:
                            is_target = True
                            td0 = tds[0]
                            checkbox = td0.find_element(By.TAG_NAME, "input")
                            checkbox.click()
    
                            btn_area = contwrap.find_element(By.CLASS_NAME, "btnArea")
                            btnsb = btn_area.find_element(By.CLASS_NAME, "btnSB")
                            btnsb.click()
                            break
                
                ## 검색 조건 설정
                self._set_search_condition()
                
                ## 재무제표 URL 목록 가져오기
                public_year_list, fs_url_list = self.get_fs_list()
                logger.info(f"공시 연도 목록: {public_year_list}")
                logger.info(f"재무제표 URL 목록: {fs_url_list}")
                
                ## 기본 페이지 드라이버 정리
                self._clean_up_driver()
                
                ## 사업보고서가 조회되지 않는 경우 실패 상태로 등록
                if len(fs_url_list) == 0:
                    logger.warning(f"{self.corp_name} 기업코드: {self.corp_code}, 종목코드: {self.stock_code}의 사업보고서가 조회되지 않습니다.")
                    try:
                        company_collection = self.mongo_client["dart"]["COMPANY"]
                        result = company_collection.update_one(
                            {"stock_code": self.stock_code},
                            {"$set": {
                                "corp_name": self.corp_name,
                                "stock_code": self.stock_code,
                                "corp_code": self.corp_code,
                                "status": "failed",
                                "message": "사업보고서가 조회되지 않음",
                                "updated_at": get_current_korea_time()
                            }},
                            upsert=True
                        )
                        logger.info(f"기업 정보 실패 상태로 등록: {self.corp_name}, upsert: {result.upserted_id is not None}")
                        return False, "사업보고서가 조회되지 않습니다", [], fs_collection_results
                    except Exception as e:
                        logger.error(f"MongoDB 저장 오류: {e}")
                        return False, f"MongoDB 저장 오류: {e}", [], fs_collection_results
            
            except Exception as e:
                logger.error(f"초기 검색 및 재무제표 URL 조회 실패: {e}")
                self._clean_up_driver()
                return False, f"초기 검색 및 재무제표 URL 조회 실패: {e}", [], fs_collection_results

            # COMPANY 컬렉션에 기업 정보 저장
            try:
                company_collection = self.mongo_client["dart"]["COMPANY"]
                logger.info(f"company 컬렉션 접근 성공")
                
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
                        "created_at": get_current_korea_time()
                    }
                    
                    # 산업 분류 정보 추가
                    if industry_levels:
                        insert_data.update(industry_levels)
                    result = company_collection.insert_one(insert_data)
                    
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
                            "updated_at": get_current_korea_time()
                        }
                    }
                    
                    # 산업 분류 정보 추가
                    if industry_levels:
                        for level_key, level_value in industry_levels.items():
                            if level_value:  # 값이 있는 경우에만 업데이트
                                update_data["$set"][level_key] = level_value
                    
                    result = company_collection.update_one(
                        {"stock_code": self.stock_code},
                        update_data
                    )
                    logger.info(f"기업 정보 업데이트: {self.corp_name}, modified: {result.modified_count}")

            except Exception as e:
                logger.error(f"MongoDB COMPANY 컬렉션 저장 오류: {e}")

            # 각 재무제표 URL에 대해 개별적으로 드라이버 생성하고 처리
            for url_idx, (public_year, fs_url) in enumerate(zip(public_year_list, fs_url_list)):
                logger.info("-" * 100)
                logger.info(f"[{url_idx+1}/{len(fs_url_list)}] 재무제표 URL 처리: {fs_url}")
                
                rcept_no = fs_url.split("=")[-1]
                bsns_year = public_year
                logger.info(f"corp_name: {self.corp_name}, bsns_year: {bsns_year}, rcept_no: {rcept_no}")
                
                # 이 URL에 대한 새 드라이버 생성
                try:
                    self.driver = self._create_driver()
                    self.driver.get(fs_url)
                    logger.info(f"재무제표 로드 성공")
                    time.sleep(1)
                    
                    ## 레이어 팝업이 있으면 닫기
                    try:
                        layer_pop_viewer = self.driver.find_element(By.CSS_SELECTOR, '#winCommMsg > div.layerPop.layerPopViewer.wF')
                        logger.info(f"레이어 팝업 발견")
                        layer_pop_title = layer_pop_viewer.find_element(By.CLASS_NAME, "title")
                        close_btn = layer_pop_title.find_element(By.CLASS_NAME, "btnClose")
                        close_btn.click()

                    except Exception as e:
                        logger.info(f"레이어 팝업 없음")

                    ## 사업보고서 페이지에서 좌측 상단 메뉴에서 사업보고서 선택
                    selector = self.driver.find_element(By.CSS_SELECTOR, '#family')
                    
                    # 현재 선택된 option 확인
                    select_element = Select(selector)
                    current_option = select_element.first_selected_option
                    current_value = current_option.get_attribute('value') if current_option else ""
                    current_title = current_option.get_attribute('title') if current_option else ""
                    
                    # 현재 선택된 option이 조건을 만족하는지 확인
                    need_to_select = not ("rcpNo=" in current_value and current_title == "사업보고서")
                    
                    if need_to_select:
                        # 조건에 맞는 option 찾기
                        options = select_element.options
                        target_option = None
                        
                        for option in options:
                            option_value = option.get_attribute('value') if option else ""
                            option_title = option.get_attribute('title') if option else ""
                            
                            if "rcpNo=" in option_value and option_title == "사업보고서":
                                target_option = option
                                break
                        
                        if target_option:
                            logger.info(f"사업보고서 option 선택: {target_option.get_attribute('value')}")
                            select_element.select_by_value(target_option.get_attribute('value'))
                            # 사업보고서 선택 후 페이지 로딩 대기
                            try:
                                WebDriverWait(self.driver, 10).until(
                                    EC.presence_of_element_located((By.CLASS_NAME, "wrapper"))
                                )
                                logger.info("사업보고서 페이지 로딩 완료")
                                time.sleep(2)  # 추가 대기
                            except TimeoutException:
                                logger.warning("사업보고서 페이지 로딩 타임아웃")
                                time.sleep(3)  # 타임아웃 시에도 대기
                        else:
                            logger.warning(f"조건에 맞는 사업보고서 option을 찾을 수 없습니다.")
                    else:
                        logger.info(f"현재 선택된 option이 이미 조건을 만족합니다: {current_title}")
                        # 이미 사업보고서가 선택되어 있어도 페이지가 완전히 로드될 때까지 대기
                        try:
                            WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.CLASS_NAME, "wrapper"))
                            )
                            logger.info("사업보고서 페이지 확인 완료")
                            time.sleep(1)
                        except TimeoutException:
                            logger.warning("사업보고서 페이지 확인 타임아웃")
                            time.sleep(2)
                    
                    try:
                        data_from_left_panel = self.left_panel_slider(self.corp_name, self.stock_code, self.corp_code, bsns_year, rcept_no)
                        if data_from_left_panel:
                            collected_data.extend(data_from_left_panel)
                            
                            for item in data_from_left_panel:
                                sj_div = item.get('sj_div')
                                if sj_div in fs_collection_results:
                                    fs_collection_results[sj_div]["status"] = "collected"
                                    fs_collection_results[sj_div]["data_count"] += 1

                    except Exception as e:
                        logger.error(f"left_panel_slider 처리 중 오류: {str(e)}")
                
                except Exception as e:
                    logger.error(f"재무제표 URL({fs_url}) 처리 중 오류: {str(e)}")
                
                finally:
                    # 각 URL 처리 후 드라이버 정리
                    self._clean_up_driver()
                    
                    # 시스템 상태 로깅
                    try:
                        # CPU 사용률
                        cpu_percent = psutil.cpu_percent(interval=0.1)
                        # 메모리 사용률
                        memory_percent = psutil.virtual_memory().percent
                        logger.info(f"시스템 상태 - CPU: {cpu_percent}%, 메모리: {memory_percent}%")
                    except Exception as e:
                        logger.error(f"시스템 상태 확인 실패: {str(e)}")
            
            # 최종 결과 반환
            if len(collected_data) > 0:
                logger.info(f"재무제표 크롤링 성공: {self.corp_name}, 수집된 데이터 수: {len(collected_data)}")
                return True, "재무제표 크롤링 성공", collected_data, fs_collection_results
            else:
                logger.warning(f"재무제표를 찾을 수 없음: {self.corp_name}")
                return False, "재무제표를 찾을 수 없습니다", [], fs_collection_results
        
        except Exception as e:
            logger.error(f"재무제표 크롤링 중 오류 발생: {e}")
            return False, str(e), [], fs_collection_results
            
        finally:
            # 최종 드라이버 정리
            self._clean_up_driver()
            
            # 명시적 가비지 컬렉션 실행
            gc.collect()