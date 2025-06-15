```bash
# Docker 이미지 빌드
docker build -t playwright_crawler_img .

# Docker 컨테이너 실행 (FastAPI 서버용)
docker run --name playwright-crawler \
    -p 8010:8010 \
    -v $(pwd):/playwright-crawler \
    --ipc=host \
    playwright_crawler_img

# 또는 직접 Python 스크립트 실행 (headless 모드 없이)
docker run --rm \
    -v $(pwd):/playwright-crawler \
    --ipc=host \
    playwright_crawler_img \
    xvfb-run -a python3 test.py
```