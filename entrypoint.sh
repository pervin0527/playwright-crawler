#!/bin/bash
set -e

# 필요한 초기화 작업 수행
echo "컨테이너 시작됨"

# 명령어 인자가 전달되면 해당 명령어 실행, 아니면 FastAPI 앱 실행
if [ "$#" -eq 0 ]; then
    echo "FastAPI 애플리케이션 시작"
    exec uvicorn main:app --host 0.0.0.0 --port 8010 --reload
else
    exec "$@"
fi