from pymongo import MongoClient
from app.src.crawler import FinancialStatementCrawler

async def main():
    crawler = FinancialStatementCrawler(
        headless=True
    )
    await crawler.collect_financial_statements(company_name="영풍제지")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())