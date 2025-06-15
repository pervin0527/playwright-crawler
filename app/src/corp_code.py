import os
import io
import zipfile
import aiofiles
import pandas as pd
import xml.etree.ElementTree as ET
import datetime

from aiohttp import ClientSession

from app.utils.logging import logger

INDUSTRY_CORPS_FILE_PATH = {
    "P": "/playwright-crawler/data/corp_overview/industry_corps_P_20250610_224859.csv",
    "A": "/playwright-crawler/data/corp_overview/industry_corps_A_20250606_114342.csv",
    "N": "/playwright-crawler/data/corp_overview/industry_corps_N_20250606_125856.csv",
    "E": "/playwright-crawler/data/corp_overview/industry_corps_E_20250607_085405.csv"
}

async def get_corp_code_df(file_path: str):
    """
    고유번호 api : https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018
    OpenDART API에서 기업 고유번호 정보를 조회하고 XML 데이터를 DataFrame으로 변환합니다.
    
    :param api_key: OpenDART API 인증키 (40자리)
    :return: 기업 고유번호 정보를 담은 Pandas DataFrame
    """
    if os.path.exists(f"{file_path}/corp_code.csv"):
        logger.info(f"{file_path}/corp_code.csv 파일이 존재. 파일 로드.")
        async with aiofiles.open(f"{file_path}/corp_code.csv", mode='r') as f:
            content = await f.read()
            return pd.read_csv(io.StringIO(content), dtype=str)

    logger.info("기업 고유번호 정보 조회 시작")
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {"crtfc_key": os.getenv("DART_API_KEY")}
    
    async with ClientSession() as session:
        async with session.get(url, params=params) as response:
            if response.status != 200:
                raise Exception(f"API 요청 실패: {response.status}")
            
            content = await response.read()
            
    with zipfile.ZipFile(io.BytesIO(content), "r") as zip_ref:
        file_name = zip_ref.namelist()[0]  # ZIP 파일 내부의 XML 파일명
        with zip_ref.open(file_name) as xml_file:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            data = []
            for corp in root.findall("list"):
                corp_code = corp.find("corp_code").text
                corp_name = corp.find("corp_name").text
                stock_code = corp.find("stock_code").text if corp.find("stock_code") is not None else "-"
                modify_date = corp.find("modify_date").text
                
                data.append({
                    "corp_code": corp_code,
                    "corp_name": corp_name,
                    "stock_code": stock_code,
                    "modify_date": modify_date
                })
            
            df = pd.DataFrame(data)
            
            # 비동기로 CSV 파일 저장
            os.makedirs(file_path, exist_ok=True)
            async with aiofiles.open(f"{file_path}/corp_code.csv", mode='w') as f:
                await f.write(df.to_csv(index=False))
            logger.info(f"{file_path}/corp_code.csv 파일 저장 완료")
            
            return df
        

def find_corp_code(corp_name, stock_code):
    """
    입력한 기업명으로 해당 기업의 고유코드(corp_code)를 찾는 함수
    corp_code가 비어있지 않은 row들만 대상으로 검색
    
    Parameters:
    corp_name (str): 찾고자 하는 기업명
    stock_code (str): 찾고자 하는 기업의 주식코드
    
    Returns:
    str: 해당 기업의 고유코드(corp_code)
        찾지 못한 경우 None 반환
    """
    corp_code_df = pd.read_csv(f"/playwright-crawler/data/corp_codes/corp_code.csv", dtype={'corp_code': str, 'stock_code': str})

    # corp_code가 비어있지 않은 row들만 필터링
    valid_df = corp_code_df[corp_code_df['corp_code'].notna() & (corp_code_df['corp_code'] != '')]
    valid_df = valid_df[valid_df['stock_code'] == stock_code]
    
    # 기업명과 정확히 일치하는 경우 검색
    exact_match = valid_df[valid_df['corp_name'] == corp_name]
    
    if not exact_match.empty:
        return exact_match.iloc[0]['corp_code']
    
    # 정확히 일치하는 경우가 없으면 부분 일치 검색
    partial_match = valid_df[valid_df['corp_name'].str.contains(corp_name, case=False, na=False)]
    
    if not partial_match.empty:
        print(f"정확한 일치 결과가 없어 부분 일치 결과를 반환합니다:")
        print(partial_match[['corp_code', 'corp_name', 'stock_code']].head())
        return partial_match.iloc[0]['corp_code']
    
    return None


def find_stock_code(corp_name, corp_type_value):
    """
    입력한 기업명으로 해당 기업의 고유코드(corp_code)를 찾는 함수
    corp_code가 비어있지 않은 row들만 대상으로 검색
    
    Parameters:
    corp_name (str): 찾고자 하는 기업명
    corp_type_value (str): 법인 유형 코드(all : 전체, P : 유가증권시장, A : 코스닥시장, N : 코넥스시장, E : 기타법인)
    
    Returns:
    str: 해당 기업의 고유코드(corp_code)
        찾지 못한 경우 None 반환
    """
    # 해당 유형의 파일이 없는 경우 처리
    if corp_type_value not in INDUSTRY_CORPS_FILE_PATH:
        # all인 경우 모든 파일을 순차적으로 검색
        if corp_type_value == "all":
            for market_type in INDUSTRY_CORPS_FILE_PATH:
                try:
                    stock_code = find_stock_code(corp_name, market_type)
                    if stock_code:
                        return stock_code
                except Exception as e:
                    logger.error(f"Error searching in {market_type} market: {e}")
                    continue
            return None
        else:
            logger.error(f"지원되지 않는 법인 유형: {corp_type_value}")
            return None
    
    try:
        # 파일 읽기
        corp_code_df = pd.read_csv(INDUSTRY_CORPS_FILE_PATH[corp_type_value], dtype={'stock_code': str})
        
        # stock_code 열을 문자열로 변환
        # corp_code_df['stock_code'] = corp_code_df['stock_code'].astype(str)
        
        # corp_code가 비어있지 않은 row들만 필터링
        valid_df = corp_code_df[
            corp_code_df['stock_code'].notna() & 
            (corp_code_df['stock_code'] != 'nan') & 
            (corp_code_df['stock_code'] != '') & 
            (corp_code_df['stock_code'].str.strip() != '')
        ]
        
        # 기업명과 정확히 일치하는 경우 검색
        exact_match = valid_df[valid_df['corp_name'] == corp_name]
        
        if not exact_match.empty:
            return exact_match.iloc[0]['stock_code']
        
        # 정확히 일치하는 경우가 없으면 부분 일치 검색
        partial_match = valid_df[valid_df['corp_name'].str.contains(corp_name, case=False, na=False)]
        
        if not partial_match.empty:
            logger.info(f"정확한 일치 결과가 없어 부분 일치 결과를 반환합니다: {partial_match.iloc[0]['corp_name']}")
            return partial_match.iloc[0]['stock_code']
        
        logger.warning(f"기업을 찾을 수 없습니다: {corp_name}")
        return None
    
    except Exception as e:
        logger.error(f"기업 코드 검색 중 오류: {e}")
        return None


def search_company(corp_name, corp_type_value):
    """
    기업명과 법인 유형 코드를 기반으로 기업 정보를 검색하는 함수
    industry_corps_*.csv 파일에서 기업 정보를 검색하고, corp_code와 산업 분류 정보 등을 함께 반환
    
    Parameters:
    corp_name (str): 검색하고자 하는 기업명
    corp_type_value (str): 법인 유형 코드(all : 전체, P : 유가증권시장, A : 코스닥시장, N : 코넥스시장, E : 기타법인)
    
    Returns:
    dict: 기업 정보 (corp_name, stock_code, corp_code, corp_type, level1, level2, level3, level4, level5)
          찾지 못한 경우 None 반환
    """
    # 해당 유형의 파일이 없는 경우 처리
    if corp_type_value not in INDUSTRY_CORPS_FILE_PATH and corp_type_value != "all":
        logger.error(f"지원되지 않는 법인 유형: {corp_type_value}")
        return None
    
    # 검색할 파일 목록 결정
    search_files = []
    if corp_type_value == "all":
        search_files = list(INDUSTRY_CORPS_FILE_PATH.items())
    else:
        if corp_type_value in INDUSTRY_CORPS_FILE_PATH:
            search_files = [(corp_type_value, INDUSTRY_CORPS_FILE_PATH[corp_type_value])]
    
    # 모든 파일을 검색
    for corp_type, file_path in search_files:
        try:
            # 파일 존재 여부 확인
            if not os.path.exists(file_path):
                logger.warning(f"파일이 존재하지 않습니다: {file_path}")
                continue
            
            # 파일 읽기
            corps_df = pd.read_csv(file_path, dtype={'stock_code': str})
            
            # 정확히 일치하는 경우 검색
            exact_match = corps_df[corps_df['corp_name'] == corp_name]
            
            if not exact_match.empty:
                row = exact_match.iloc[0]
                stock_code = row['stock_code']
                corp_code = find_corp_code(row['corp_name'], stock_code)
                
                return {
                    'corp_name': row['corp_name'],
                    'stock_code': stock_code,
                    'corp_code': corp_code,
                    'corp_type': corp_type,
                    'level1': row.get('level1', ''),
                    'level2': row.get('level2', ''),
                    'level3': row.get('level3', ''),
                    'level4': row.get('level4', ''),
                    'level5': row.get('level5', '')
                }
            
            # 부분 일치 검색
            partial_match = corps_df[corps_df['corp_name'].str.contains(corp_name, case=False, na=False)]
            
            if not partial_match.empty:
                row = partial_match.iloc[0]
                stock_code = row['stock_code']
                corp_code = find_corp_code(row['corp_name'], stock_code)
                
                return {
                    'corp_name': row['corp_name'],
                    'stock_code': stock_code,
                    'corp_code': corp_code,
                    'corp_type': corp_type,
                    'level1': row.get('level1', ''),
                    'level2': row.get('level2', ''),
                    'level3': row.get('level3', ''),
                    'level4': row.get('level4', ''),
                    'level5': row.get('level5', '')
                }
        
        except Exception as e:
            logger.error(f"{corp_type} 시장에서 기업 정보 검색 중 오류 발생: {e}")
            continue
    
    logger.warning(f"기업을 찾을 수 없습니다: 기업명={corp_name}, 법인유형={corp_type_value}")
    return None