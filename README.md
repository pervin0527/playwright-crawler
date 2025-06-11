```bash
docker build -t playwright_crawler_img .

docker run --name playwright-crawler \
    -p 8010:8010 \
    -e DISPLAY=$DISPLAY \
    -v /tmp/.X11-unix:/tmp/.X11-unix \
    -v $(pwd):/playwright-crawler \
    --ipc=host \
    playwright_crawler_img
```