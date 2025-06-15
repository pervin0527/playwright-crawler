#!/bin/bash
set -e

# Xvfb 가상 디스플레이 서버 시작 함수
start_xvfb() {
    echo "Xvfb 가상 디스플레이 서버 시작 중..."
    # 기존 Xvfb 프로세스가 있다면 종료
    pkill -f "Xvfb :99" || true
    
    # Xvfb 시작
    Xvfb :99 -screen 0 1920x1080x24 -ac +extension RANDR &
    XVFB_PID=$!
    
    # DISPLAY 환경변수 설정
    export DISPLAY=:99
    
    # Xvfb가 시작될 때까지 대기
    echo "Xvfb 시작 대기 중..."
    sleep 3
    
    # Xvfb가 제대로 시작되었는지 확인
    if ! pgrep -f "Xvfb :99" > /dev/null; then
        echo "ERROR: Xvfb 시작에 실패했습니다."
        exit 1
    fi
    
    echo "Xvfb 시작 완료 (PID: $XVFB_PID, DISPLAY: $DISPLAY)"
}

# 필요한 경우 Xvfb 시작
if [ "$ENABLE_XVFB" != "false" ]; then
    start_xvfb
fi

# 필요한 초기화 작업 수행
echo "컨테이너 시작됨"

# 명령어 인자가 전달되면 해당 명령어 실행, 아니면 FastAPI 앱 실행
if [ "$#" -eq 0 ]; then
    echo "FastAPI 애플리케이션 시작"
    exec uvicorn app.main:app --host 0.0.0.0 --port 8010 --reload
else
    exec "$@"
fi