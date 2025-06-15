FROM mcr.microsoft.com/playwright:v1.52.0-jammy

ARG DEBIAN_FRONTEND=noninteractive

# 환경 변수 설정
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUTF8=1 \
    PIP_NO_CACHE_DIR=on \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PYTHONPATH=/culture_backoffice_data

# Python 및 pip 설치 추가
RUN apt-get update -q && \
    apt-get install -y -q \
      python3 \
      python3-pip \
      python3-venv \
      python3-dev \
      wget \
      unzip \
      curl \
      gnupg2 \
      ca-certificates \
      libglib2.0-0 \
      libnss3 \
      libgconf-2-4 \
      libfontconfig1 \
      libxi6 \
      libxcursor1 \
      libxss1 \
      libxcomposite1 \
      libasound2 \
      libxdamage1 \ 
      libxtst6 \
      libatk1.0-0 \
      libgtk-3-0 \
      libdrm2 \
      libgbm1 \
      fonts-liberation \
      libu2f-udev \
      libvulkan1 \
      xdg-utils \
      tini \
      fonts-nanum \
      libmagic1 \
      libopenmpi-dev \
      git \
      build-essential \
      automake \
      mecab \
      libmecab-dev \
      mecab-ipadic-utf8 \
      xvfb \
      x11vnc \
      fluxbox \
      --no-install-recommends && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

    
WORKDIR /playwright-crawler
COPY . /playwright-crawler
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --no-cache-dir -r requirements.txt

ENV DISPLAY=:99
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# 스크린샷 디렉토리 생성
RUN mkdir -p /playwright-crawler/screenshots
ENV SCREENSHOT_DIR=/playwright-crawler/screenshots

# 8010 포트 노출
EXPOSE 8010

ENTRYPOINT ["/playwright-crawler/entrypoint.sh"]