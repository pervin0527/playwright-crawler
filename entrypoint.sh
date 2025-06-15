#!/bin/bash
set -e

# Xvfb 가상 디스플레이 서버 시작
echo "Xvfb 가상 디스플레이 서버 시작 중..."
Xvfb :99 -screen 0 1920x1080x24 -ac &
export DISPLAY=:99

# Xvfb가 시작될 때까지 잠시 대기
sleep 2

# 필요한 초기화 작업 수행
echo "컨테이너 시작됨"

# 명령어 인자가 전달되면 해당 명령어 실행, 아니면 FastAPI 앱 실행
if [ "$#" -eq 0 ]; then
    echo "FastAPI 애플리케이션 시작"
    exec uvicorn app.main:app --host 0.0.0.0 --port 8010 --reload
else
    exec "$@"
fi