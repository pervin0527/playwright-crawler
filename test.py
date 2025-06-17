import time
import asyncio
import json
from typing import Dict, List, Any, Set

from app.src.corp_code import search_company
from app.src.crawler import FinancialStatementCrawler as PlaywrightCrawler
from app.src.past_version_crawler import FinancialStatementCrawler as SeleniumCrawler

def analyze_data_structure(dataset: List[Dict], crawler_name: str) -> Dict[str, Any]:
    """데이터 구조를 분석하는 함수"""
    print(f"\n{'='*20} {crawler_name} 데이터 구조 분석 {'='*20}")
    
    if not dataset:
        print("❌ 데이터가 없습니다.")
        return {
            "fields": set(), 
            "data_fields": set(), 
            "sample_data": None,
            "total_count": 0,
            "data_items_count": 0
        }
    
    # 최상위 필드 분석
    top_level_fields = set()
    data_level_fields = set()
    sample_data = None
    
    for item in dataset:
        if isinstance(item, dict):
            # 최상위 필드들 수집
            top_level_fields.update(item.keys())
            
            # data 필드 내부 구조 분석
            if 'data' in item and isinstance(item['data'], list) and item['data']:
                for data_item in item['data']:
                    if isinstance(data_item, dict):
                        data_level_fields.update(data_item.keys())
                        if sample_data is None:
                            sample_data = data_item
    
    print(f"📋 최상위 필드들: {sorted(top_level_fields)}")
    print(f"📋 data 필드 내부 구조: {sorted(data_level_fields)}")
    
    # 샘플 데이터 출력
    if dataset:
        print(f"📄 샘플 최상위 데이터:")
        sample_top = {k: v for k, v in dataset[0].items() if k != 'data'}
        for key, value in sample_top.items():
            print(f"   {key}: {value}")
        
        if sample_data:
            print(f"📄 샘플 data 내부 구조:")
            for key, value in sample_data.items():
                if key == 'amounts':
                    if isinstance(value, dict):
                        print(f"   {key}: {dict(list(value.items())[:2])}...")  # dict 형태일 때
                    elif isinstance(value, list):
                        print(f"   {key}: {value[:2]}...")  # list 형태일 때
                    else:
                        print(f"   {key}: {value}")
                else:
                    print(f"   {key}: {value}")
    
    return {
        "fields": top_level_fields,
        "data_fields": data_level_fields,
        "sample_data": sample_data,
        "total_count": len(dataset),
        "data_items_count": sum(len(item.get('data', [])) for item in dataset if isinstance(item, dict))
    }

def compare_data_structures(playwright_analysis: Dict, selenium_analysis: Dict):
    """두 크롤러의 데이터 구조를 비교하는 함수"""
    print(f"\n{'🔍'*25} 데이터 구조 비교 {'🔍'*25}")
    
    # 최상위 필드 비교
    playwright_fields = playwright_analysis["fields"]
    selenium_fields = selenium_analysis["fields"]
    
    common_fields = playwright_fields & selenium_fields
    playwright_only = playwright_fields - selenium_fields
    selenium_only = selenium_fields - playwright_fields
    
    print(f"🔗 공통 최상위 필드: {sorted(common_fields)}")
    if playwright_only:
        print(f"🎭 Playwright 전용 필드: {sorted(playwright_only)}")
    if selenium_only:
        print(f"🌐 Selenium 전용 필드: {sorted(selenium_only)}")
    
    # data 필드 내부 구조 비교
    playwright_data_fields = playwright_analysis["data_fields"]
    selenium_data_fields = selenium_analysis["data_fields"]
    
    common_data_fields = playwright_data_fields & selenium_data_fields
    playwright_data_only = playwright_data_fields - selenium_data_fields
    selenium_data_only = selenium_data_fields - playwright_data_fields
    
    print(f"\n🔗 공통 data 내부 필드: {sorted(common_data_fields)}")
    if playwright_data_only:
        print(f"🎭 Playwright data 전용 필드: {sorted(playwright_data_only)}")
    if selenium_data_only:
        print(f"🌐 Selenium data 전용 필드: {sorted(selenium_data_only)}")
    
    # 데이터 수량 비교
    print(f"\n📊 수집 데이터 수량 비교:")
    print(f"   🎭 Playwright: {playwright_analysis['total_count']}개 재무제표, {playwright_analysis['data_items_count']}개 계정 항목")
    print(f"   🌐 Selenium: {selenium_analysis['total_count']}개 재무제표, {selenium_analysis['data_items_count']}개 계정 항목")
    
    # 구조 호환성 검사
    structure_compatibility = True
    issues = []
    
    # 핵심 필드 존재 여부 확인
    required_top_fields = {'sj_div', 'bsns_year', 'unit', 'data'}
    required_data_fields = {'ord_value', 'raw_account_name', 'account_name', 'amounts', 'account_level', 'ancestors'}
    
    for field in required_top_fields:
        if field not in common_fields:
            structure_compatibility = False
            issues.append(f"핵심 최상위 필드 '{field}' 누락")
    
    for field in required_data_fields:
        if field not in common_data_fields:
            structure_compatibility = False
            issues.append(f"핵심 data 필드 '{field}' 누락")
    
    print(f"\n✅ 구조 호환성: {'호환 가능' if structure_compatibility else '호환 불가능'}")
    if issues:
        for issue in issues:
            print(f"   ⚠️  {issue}")
    
    return {
        "compatible": structure_compatibility,
        "common_fields": common_fields,
        "common_data_fields": common_data_fields,
        "issues": issues
    }

def validate_data_consistency(playwright_data: List[Dict], selenium_data: List[Dict]):
    """데이터 일관성을 검증하는 함수"""
    print(f"\n{'📋'*25} 데이터 일관성 검증 {'📋'*25}")
    
    # 재무제표 유형별 분류
    playwright_by_type = {}
    selenium_by_type = {}
    
    for item in playwright_data:
        sj_div = item.get('sj_div', 'UNKNOWN')
        if sj_div not in playwright_by_type:
            playwright_by_type[sj_div] = []
        playwright_by_type[sj_div].append(item)
    
    for item in selenium_data:
        sj_div = item.get('sj_div', 'UNKNOWN')
        if sj_div not in selenium_by_type:
            selenium_by_type[sj_div] = []
        selenium_by_type[sj_div].append(item)
    
    print(f"🎭 Playwright 재무제표 유형: {sorted(playwright_by_type.keys())}")
    print(f"🌐 Selenium 재무제표 유형: {sorted(selenium_by_type.keys())}")
    
    # 공통 재무제표 유형에 대한 상세 비교
    common_types = set(playwright_by_type.keys()) & set(selenium_by_type.keys())
    if common_types:
        print(f"\n🔍 공통 재무제표 유형별 상세 비교:")
        for fs_type in sorted(common_types):
            pw_items = playwright_by_type[fs_type]
            sel_items = selenium_by_type[fs_type]
            
            print(f"\n📊 {fs_type}:")
            print(f"   🎭 Playwright: {len(pw_items)}개")
            print(f"   🌐 Selenium: {len(sel_items)}개")
            
            # 각 재무제표별 계정 수 비교
            for i, (pw_item, sel_item) in enumerate(zip(pw_items, sel_items)):
                pw_accounts = len(pw_item.get('data', []))
                sel_accounts = len(sel_item.get('data', []))
                pw_year = pw_item.get('bsns_year', 'N/A')
                sel_year = sel_item.get('bsns_year', 'N/A')
                
                print(f"     연도 {pw_year}/{sel_year}: 🎭{pw_accounts}개, 🌐{sel_accounts}개 계정")
                
                # 계정명 비교 (처음 5개만)
                pw_account_names = [acc.get('account_name', '') for acc in pw_item.get('data', [])][:5]
                sel_account_names = [acc.get('account_name', '') for acc in sel_item.get('data', [])][:5]
                
                common_accounts = set(pw_account_names) & set(sel_account_names)
                if common_accounts:
                    print(f"       공통 계정명 (샘플): {list(common_accounts)[:3]}")
    
    # 누락된 재무제표 유형 확인
    playwright_only_types = set(playwright_by_type.keys()) - set(selenium_by_type.keys())
    selenium_only_types = set(selenium_by_type.keys()) - set(playwright_by_type.keys())
    
    if playwright_only_types:
        print(f"\n🎭 Playwright에서만 수집된 재무제표: {sorted(playwright_only_types)}")
    if selenium_only_types:
        print(f"🌐 Selenium에서만 수집된 재무제표: {sorted(selenium_only_types)}")
    
    return {
        "common_types": common_types,
        "playwright_only": playwright_only_types,
        "selenium_only": selenium_only_types,
        "playwright_by_type": playwright_by_type,
        "selenium_by_type": selenium_by_type
    }

async def test_playwright_crawler(corp_name: str, corp_type_value: str):
    """Playwright 기반 크롤러 테스트"""
    print("=" * 60)
    print("🎭 Playwright 크롤러 테스트 시작")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        crawler = PlaywrightCrawler(headless=True)
        dataset = await crawler.collect_financial_statements(company_name=corp_name, corp_type_value=corp_type_value)
        
        print(f"📊 총 수집된 데이터셋 수: {len(dataset)}")
        
        for data in dataset:
            print(f"  - {data['bsns_year']}년 {data['sj_div']}: {len(data['data'])}개 항목")
            # 연도별 금액 데이터 확인
            years_data = {}
            for item in data['data']:
                amounts = item.get('amounts', [])
                # amounts가 list 형태로 변경됨: [{'2024': '0'}, {'2023': '0'}, {'2022': '0'}]
                if isinstance(amounts, list):
                    for amount_dict in amounts:
                        if isinstance(amount_dict, dict):
                            for year, amount in amount_dict.items():
                                if year not in years_data:
                                    years_data[year] = 0
                                if amount and amount != "0":
                                    years_data[year] += 1
                # 이전 형태 호환성 유지 (dict 형태)
                elif isinstance(amounts, dict):
                    for year, amount in amounts.items():
                        if year not in years_data:
                            years_data[year] = 0
                        if amount and amount != "0":
                            years_data[year] += 1
            print(f"    연도별 데이터: {years_data}")
        
        end_time = time.time()
        playwright_time = end_time - start_time
        
        print(f"⏱️  Playwright 크롤러 소요 시간: {playwright_time:.2f}초")
        print("✅ Playwright 크롤러 테스트 완료")
        
        return playwright_time, len(dataset), dataset
        
    except Exception as e:
        print(f"❌ Playwright 크롤러 오류: {e}")
        return None, 0, []

def test_selenium_crawler(corp_name: str, stock_code: str, corp_code: str, corp_type_value: str):
    """Selenium 기반 크롤러 테스트"""
    print("\n" + "=" * 60)
    print("🌐 Selenium 크롤러 테스트 시작")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        crawler = SeleniumCrawler(
            corp_name=corp_name,
            stock_code=stock_code,
            corp_code=corp_code,
            corp_type_value=corp_type_value,
            headless=True
        )
        
        success, message, collected_data, fs_collection_results = crawler.get_corp_fs()
        
        print(f"📊 크롤링 성공: {success}")
        print(f"📝 메시지: {message}")
        print(f"📊 총 수집된 데이터 수: {len(collected_data)}")
        
        end_time = time.time()
        selenium_time = end_time - start_time
        
        print(f"⏱️  Selenium 크롤러 소요 시간: {selenium_time:.2f}초")
        print("✅ Selenium 크롤러 테스트 완료")
        
        return selenium_time, len(collected_data), collected_data
        
    except Exception as e:
        print(f"❌ Selenium 크롤러 오류: {e}")
        return None, 0, []

async def compare_crawlers():
    """두 크롤러의 성능을 비교"""
    company_name = "세아제강"
    corp_type_value = "P"
    search_result = search_company(corp_name=company_name, corp_type_value=corp_type_value)
    stock_code = search_result['stock_code']
    corp_code = search_result['corp_code']

    print("\n" + "🔥" * 20 + " 크롤러 성능 비교 테스트 " + "🔥" * 20)
    print(f"🏢 테스트 대상 기업: {company_name} (종목코드: {stock_code})")
    print(f"📅 테스트 시작 시간: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Playwright 크롤러 테스트
    playwright_time, playwright_data_count, playwright_data = await test_playwright_crawler(corp_name=company_name, corp_type_value=corp_type_value)
    
    # 잠시 대기 (시스템 안정화)
    print("\n⏳ 시스템 안정화를 위해 3초 대기...")
    time.sleep(3)
    
    # Selenium 크롤러 테스트
    selenium_time, selenium_data_count, selenium_data = test_selenium_crawler(corp_name=company_name, stock_code=stock_code, corp_code=corp_code, corp_type_value=corp_type_value)
    
    # 데이터 구조 분석
    if playwright_data or selenium_data:
        print("\n" + "📊" * 20 + " 데이터 구조 분석 시작 " + "📊" * 20)
        
        playwright_analysis = analyze_data_structure(playwright_data, "Playwright")
        selenium_analysis = analyze_data_structure(selenium_data, "Selenium")
        
        # 데이터 구조 비교
        comparison_result = compare_data_structures(playwright_analysis, selenium_analysis)
        
        # 데이터 일관성 검증
        if playwright_data and selenium_data:
            consistency_result = validate_data_consistency(playwright_data, selenium_data)
    
    # 결과 비교
    print("\n" + "🏆" * 20 + " 최종 비교 결과 " + "🏆" * 20)
    
    if playwright_time and selenium_time:
        print(f"📊 성능 비교:")
        print(f"  🎭 Playwright: {playwright_time:.2f}초 ({playwright_data_count}개 데이터셋)")
        print(f"  🌐 Selenium:   {selenium_time:.2f}초 ({selenium_data_count}개 데이터셋)")
        
        time_diff = abs(playwright_time - selenium_time)
        faster_crawler = "Playwright" if playwright_time < selenium_time else "Selenium"
        slower_time = max(playwright_time, selenium_time)
        faster_time = min(playwright_time, selenium_time)
        
        if slower_time > 0:
            improvement_percent = (time_diff / slower_time) * 100
            print(f"  ⚡ {faster_crawler}가 {time_diff:.2f}초 더 빠름 ({improvement_percent:.1f}% 개선)")
        
        print(f"  📈 데이터 수집량: Playwright {playwright_data_count}개, Selenium {selenium_data_count}개")
        
        if playwright_data_count == selenium_data_count:
            print("  ✅ 두 크롤러 모두 동일한 양의 데이터를 수집했습니다.")
        else:
            print("  ⚠️  수집된 데이터 양이 다릅니다.")
    
    else:
        print("❌ 일부 크롤러에서 오류가 발생하여 정확한 비교가 불가능합니다.")
    
    print("\n" + "✨" * 25 + " 테스트 완료 " + "✨" * 25)

if __name__ == "__main__":
    asyncio.run(compare_crawlers())