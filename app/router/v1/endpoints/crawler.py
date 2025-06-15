from fastapi import APIRouter

from app.src.crawler import PlaywrightFinancialStatementCrawler
from app.src.corp_code import search_company

router = APIRouter()

@router.post("/crawler/company_fs")
async def collect_company_fs(
    corp_name: str,
    corp_type_value: str,
    retry_count: int = 3
):
    
    search_result = search_company(corp_name, corp_type_value)
    
    if search_result is None:
        return {"message": "failed", "message": "검색 결과가 없습니다."}
    
    stock_code = search_result["stock_code"]
    corp_code = search_result["corp_code"]
    corp_type = search_result["corp_type"]
    level1 = search_result["level1"]
    level2 = search_result["level2"]
    level3 = search_result["level3"]
    level4 = search_result["level4"]
    level5 = search_result["level5"]

    crawler = PlaywrightFinancialStatementCrawler(
        corp_name=corp_name,
        stock_code=stock_code,
        corp_type_value=corp_type_value,
        retry_count=retry_count)
    
    success, message, years, urls = await crawler.initialize_and_search()
    
    if success:
        return {"message": "success", "years": years, "urls": urls}
    else:
        return {"message": "failed", "message": message}