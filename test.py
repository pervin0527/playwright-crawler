from app.src.crawler import FinancialStatementCrawler


async def main():
    crawler = FinancialStatementCrawler(headless=True)
    await crawler.collect_financial_statements("형지엘리트")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())