import re
import pandas as pd

from typing import List, Tuple


def extract_year_from_report_title(title: str) -> str:
    """
    사업보고서 제목에서 연도(YYYY)를 추출하는 함수.
    
    다음과 같은 형식의 제목에서 작동합니다:
    - 사업보고서 (2024.06)
    - 사업보고서제출기한연장신고서 (2021.06)
    - [기재정정]사업보고서 (2019.06)
    
    Args:
        title (str): 사업보고서 제목
        
    Returns:
        str: 추출된 연도 (YYYY) 또는 추출 실패 시 None
    """
    if not title or not isinstance(title, str):
        return None
    
    # 괄호 안의 연도.월 패턴을 찾음 (2024.06)
    match = re.search(r'\((\d{4})\.?\d{2}\)', title)
    if match:
        return match.group(1)  # 첫 번째 캡처 그룹 (연도 부분)
    
    # 괄호가 없는 경우 직접 연도.월 패턴을 찾음
    match = re.search(r'(\d{4})\.?\d{2}', title)
    if match:
        return match.group(1)
    
    return None


def clean_account_name(text: str) -> str:
    """
    계정명 정제:
    - (단위:원), "(주…)", 로마숫자, 숫자·특수문자, 모든 괄호 제거
    - 전각·반각 공백 제거
    - 가., 나., 다. 등의 접두어 제거
    - 단어 사이 공백 제거 ('재 무 상 태 표' → '재무상태표')
    """
    if not isinstance(text, str) or not text:
        return ""

    # 1) 좌·우 공백 제거
    text = text.strip()

    # 2) 단위·따옴표 제거
    text = text.replace('(단위:원)', '').replace('"', '')

    # 3) 로마숫자·유니코드 로마숫자 제거
    text = re.sub(r'\b[IVXLCDM]+\b\.?', '', text)         # ASCII
    text = re.sub(r'[\u2160-\u2188]', '', text)           # Unicode (ⅠⅡⅢ …)

    # 4) (주 …) 형태 제거
    text = re.sub(r'\(주[0-9,\s]*\)', '', text)

    # 5) 남은 **모든** 괄호 & 괄호 안 내용 제거 → 불필요한 빈·참조 괄호 제거
    text = re.sub(r'\([^)]*\)', '', text)                 # 반각 ()
    text = re.sub(r'（[^）]*）', '', text)                 # 전각 （）

    # 6) 가., 나., 다. 등의 접두어 제거
    text = re.sub(r'[가-힣]\.\s*', '', text)

    # 7) 숫자·점·대시·특수문자 삭제
    text = re.sub(r'[0-9.\-_【】]', '', text)

    # 8) 전각 공백 제거
    text = text.replace('\u3000', '')
    
    # 9) 모든 공백 제거 (단어 사이 공백 포함)
    text = re.sub(r'\s+', '', text)

    return text


def clean_paragraph_text(text: str) -> str:
    """
    텍스트 정제:
    - (단위:원), "(주…)", 로마숫자, 숫자·특수문자, 모든 괄호 제거
    - 전각·반각 공백 제거
    - 가., 나., 다. 등의 접두어 제거
    - 단어 사이 공백 제거 ('재 무 상 태 표' → '재무상태표')
    """
    if not isinstance(text, str) or not text:
        return ""

    # 1) 좌·우 공백 제거
    text = text.strip()

    # 2) 단위·따옴표 제거
    text = text.replace('(단위:원)', '').replace('"', '')

    # 3) 로마숫자·유니코드 로마숫자 제거
    text = re.sub(r'\b[IVXLCDM]+\b\.?', '', text)         # ASCII
    text = re.sub(r'[\u2160-\u2188]', '', text)           # Unicode (ⅠⅡⅢ …)

    # 4) (주 …) 형태 제거
    text = re.sub(r'\(주[0-9,\s]*\)', '', text)

    # 5) 남은 **모든** 괄호 & 괄호 안 내용 제거 → 불필요한 빈·참조 괄호 제거
    text = re.sub(r'\([^)]*\)', '', text)                 # 반각 ()
    text = re.sub(r'（[^）]*）', '', text)                 # 전각 （）

    # 6) 가., 나., 다. 등의 접두어 제거 (행의 시작이 아닐 수도 있어 수정)
    text = re.sub(r'[가-힣]\.\s*', '', text)

    # 7) 숫자·점·대시·특수문자 삭제
    text = re.sub(r'[0-9.\-_【】]', '', text)

    # 8) 전각 공백 제거
    text = text.replace('\u3000', '')
    
    # 9) 모든 공백 제거 (단어 사이 공백 포함)
    text = re.sub(r'\s+', '', text)

    return text


def extract_year(text):
    """
    다양한 형태의 재무제표 기간 표시 문자열에서 연도를 추출합니다.

    Args:
        text (str): 재무제표 기간 표시 문자열

    Returns:
        str: 추출된 연도 문자열 (예: '2024'), 실패 시 None
    """
    # '현재' 패턴 (예: '제 64 기          2015.12.31 현재')
    if '현재' in text:
        match = re.search(r'(\d{4})\.\d{2}\.\d{2}\s*현재', text)
        if match:
            return match.group(1)  # 연도만 반환

    # '부터 ~ 까지' 패턴 (예: '제 64 기 2015.01.01 부터 2015.12.31 까지')
    elif '부터' in text and '까지' in text:
        matches = re.findall(r'(\d{4})\.\d{2}\.\d{2}', text)
        if len(matches) >= 2:
            return matches[1]  # 종료 연도만 반환

    return None


def extract_year_from_dart_url(url: str) -> str:
    """
    DART 전자공시 URL에서 연도(YYYY)를 추출하는 함수.
    예: https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20150921000093 → '2015'
    """
    match = re.search(r'rcpNo=(\d{4})\d{10}', url)
    if match:
        return match.group(1)
    else:
        raise ValueError("유효한 rcpNo 형식을 찾을 수 없습니다.")
    

def str_to_number(amount_str):
    """
    문자열 형태의 금액을 숫자로 변환합니다.
    괄호로 표시된 음수 표기(예: (1,000))도 처리합니다.
    
    Args:
        amount_str: 변환할 금액 문자열
        
    Returns:
        float: 변환된 숫자
    """
    if amount_str == 0:
        return 0
    if isinstance(amount_str, (int, float)):
        return amount_str
    
    # 문자열로 변환
    amount_str = str(amount_str).strip()
    
    # 괄호로 둘러싸인 경우 음수로 처리 (예: (1,000) -> -1000)
    is_negative = False
    if amount_str.startswith('(') and amount_str.endswith(')'):
        amount_str = amount_str[1:-1]  # 괄호 제거
        is_negative = True
    
    # 콤마 제거 후 숫자로 변환
    try:
        value = float(amount_str.replace(',', ''))
        return -value if is_negative else value
    except (ValueError, TypeError):
        return 0
    

def extract_years_and_amounts(data: List[dict]) -> Tuple[List[str], List[int]]:
    output = []
    for item in data:
        for year, amount in item.items():
            output.append((year, amount))
    return output


def string_to_float(value_str):
    """
    문자열 금액을 float 값으로 변환하는 함수 (괄호 처리 포함)
    
    Args:
        value_str: 변환할 문자열 금액
    
    Returns:
        float: 변환된 숫자 금액
    """
    if pd.isna(value_str) or value_str == '' or value_str == '0' or value_str == 0:
        return 0.0
        
    # 문자열로 변환
    str_value = str(value_str).strip()
    
    # 괄호로 둘러싸인 숫자는 음수로 처리 (e.g. '(1471026499)' -> '-1471026499')
    if str_value.startswith('(') and str_value.endswith(')'):
        str_value = '-' + str_value[1:-1]
    
    # 쉼표와 공백 제거
    str_value = str_value.replace(',', '').replace(' ', '')
    
    return float(str_value)


def float_to_formatted_string(value):
    """
    숫자 값을 쉼표가 포함된 문자열로 변환하는 함수
    
    Args:
        value: 변환할 float 또는 int 값
    
    Returns:
        str: 쉼표가 포함된 문자열 형식의 금액
    """
    if pd.isna(value) or value == '' or value == 0 or value == '0':
        return '0'
    
    # 음수 처리
    if float(value) < 0:
        return f"({abs(float(value)):,.0f})"
    else:
        return f"{float(value):,.0f}"
    

def convert_to_number(value):
    """문자열 또는 숫자 값을 숫자로 변환하는 함수"""
    if pd.isna(value) or value == '':
        return 0
        
    if isinstance(value, (int, float)):
        return value
        
    # 쉼표 제거 후 변환 시도
    try:
        return float(str(value).replace(',', ''))
    except:
        return 0
    

# JSON 직렬화 불가능한 값(NaN, Infinity 등) 처리
def sanitize_json_values(obj):
    if isinstance(obj, dict):
        return {k: sanitize_json_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_json_values(item) for item in obj]
    elif isinstance(obj, float):
        # NaN, Infinity, -Infinity 값을 null로 대체
        if pd.isna(obj) or obj == float('inf') or obj == float('-inf'):
            return None
        return obj
    else:
        return obj