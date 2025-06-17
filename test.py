import time

from pymongo import MongoClient
from app.src.crawler import FinancialStatementCrawler

async def main():
    start_time = time.time()
    crawler = FinancialStatementCrawler(headless=True)
    dataset = await crawler.collect_financial_statements(company_name="형지엘리트")
    print(len(dataset))

    for data in dataset:
        print(data['bsns_year'], data['sj_div'], data['years'], data['unit'], len(data['data']))

    end_time = time.time()
    print(f"소요 시간: {end_time - start_time}초")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())