import os
import logging

# 현재 파일의 위치를 기준으로 프로젝트 루트 디렉토리 경로 설정
project_root = os.path.dirname(os.path.abspath(__file__))

# 루트 디렉토리에 logs 폴더 생성
log_directory = os.path.join(project_root, "logs")

# 디렉토리가 존재하지 않으면 생성
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# 로거 설정
logger = logging.getLogger("Dart Data Logger")
logger.setLevel(logging.INFO)

# 중복 로그 방지: 기존 핸들러가 있으면 제거
if logger.hasHandlers():
    logger.handlers.clear()

# 파일 핸들러를 통해 로그 파일로 출력
log_file_path = os.path.join(log_directory, "dart.log")
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.INFO)

# 콘솔 핸들러 추가
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# 포맷 설정
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# 로거에 핸들러 추가
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 중복 로그 방지
logger.propagate = False