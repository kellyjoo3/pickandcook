from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker # <- import 시도
from sqlalchemy.ext.declarative import declarative_base # <- declarative_base import 추가
import logging # <- logging import 추가
import sys # 진단용

# 로깅 설정 (파일 로드 확인용)
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] - %(message)s')
logging.info("--- database.py 로딩 시작 ---")

print(f"--- Python 버전: {sys.version}")
print(f"--- SQLAlchemy 경로: {sys.modules.get('sqlalchemy', 'Not Found')}")

# --- import 직후 sessionmaker 타입 확인 ---
try:
    #print(f"DEBUG (Import 직후): sessionmaker 타입: {type(sessionmaker)}")
    if 'sessionmaker' not in str(type(sessionmaker)).lower():
         print("!!! WARNING: Import 직후 sessionmaker 타입 이상 !!!")
except NameError:
    print("!!! ERROR: Import 직후 sessionmaker NameError 발생 !!!")
except Exception as e:
    print(f"!!! ERROR: Import 직후 타입 확인 중 예외: {e}")
# --- 확인 끝 ---

SQLALCHEMY_DATABASE_URL = "sqlite:///./recipes.db"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)

# --- SessionLocal 생성 직전 sessionmaker 타입 재확인 ---
try:
    #print(f"DEBUG (사용 직전): sessionmaker 타입: {type(sessionmaker)}")
    if 'sessionmaker' not in str(type(sessionmaker)).lower():
         print("!!! WARNING: 사용 직전 sessionmaker 타입 이상 !!!")
    # --- 실제 사용 ---
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    logging.info("SessionLocal 생성 성공.")

    # --- SessionLocal 타입 확인 코드 추가 ---
    #print(f"DEBUG: Created SessionLocal type: {type(SessionLocal)}")
    # --- SessionLocal 타입 확인 코드 추가 ---
except NameError:
    logging.error("!!! CRITICAL ERROR: SessionLocal 생성 시점에서 sessionmaker NameError 발생 !!!")
    raise # 오류 발생시켜 프로그램 중지
except Exception as e:
    logging.error("!!! CRITICAL ERROR: SessionLocal 생성 중 예외 발생: {e}")
    raise
# --- 확인 끝 ---

# Base = declarative_base() # 여전히 주석 처리

logging.info("--- database.py 로딩 완료 ---")
# 아래 줄이 실수로 포함되어 SyntaxError 발생 -> 제거함
# python database.py

