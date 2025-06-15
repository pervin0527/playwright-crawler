import pytz
import time

from datetime import datetime

from app.utils.logging import logger


def get_current_korea_time():
    korea_timezone = pytz.timezone('Asia/Seoul')
    korea_time = datetime.now(korea_timezone)
    return korea_time


async def measure_execution_time(func_name, async_func, *args, **kwargs):
    """특정 비동기 함수의 실행 시간을 측정하는 유틸리티 함수"""
    start_time = time.time()
    result = await async_func(*args, **kwargs)
    execution_time = time.time() - start_time
    logger.info(f"[EXECUTION_TIME] {func_name} 실행 시간: {execution_time:.4f}초")
    return result, execution_time