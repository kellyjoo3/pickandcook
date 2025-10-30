import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Replit Secrets(환경 변수)에서 DATABASE_URL을 읽어옴
SQLALCHEMY_DATABASE_URL = os.environ.get("DATABASE_URL")

if not SQLALCHEMY_DATABASE_URL:
    logging.warning("DATABASE_URL이 설정되지 않았습니다. 로컬 SQLite로 대체합니다.")
    SQLALCHEMY_DATABASE_URL = "sqlite:///./recipes.db"  # 로컬 테스트용
else:
    logging.info("Cloud PostgreSQL (Neon) DB에 연결을 시도합니다.")

# create_engine 설정
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# DB 세션 팩토리 생성
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ★★★★★
# SQLAlchemy 모델(테이블 클래스)들의 기반이 될 Base 클래스
# init_db.py가 이 'Base'를 import합니다.
Base = declarative_base()
# ★★★★★

logging.info(
    f"--- database.py 로드 완료 (DB: {SQLALCHEMY_DATABASE_URL[:30]}...) ---")
