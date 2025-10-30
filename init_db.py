import logging
import time
# --- ▼▼▼ 필요한 모든 SQLAlchemy 모듈 import ▼▼▼ ---
from sqlalchemy import create_engine, Column, String, Boolean, Text, ForeignKey, Integer, DateTime, func
# --- ▲▲▲ 필요한 모든 SQLAlchemy 모듈 import ▲▲▲ ---

# 1. database.py에서 Cloud DB용 engine과 Base를 가져옵니다.
try:
    # (database.py가 깨끗한 최종본 상태여야 합니다)
    from database import engine, Base
    logging.info("database.py에서 engine과 Base를 성공적으로 가져왔습니다.")
except ImportError as e:
    print("=" * 50)
    print(f"[오류] database.py에서 engine 또는 Base를 가져오는 데 실패했습니다: {e}")
    print("database.py 파일이 올바르게 설정되었는지 확인하세요.")
    print("=" * 50)
    exit()

# 2. 로깅 설정
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] [%(levelname)s] - %(message)s')

# --- 테이블 설계 ---


class Channel(Base):
    __tablename__ = 'channels'
    channel_id = Column(String, primary_key=True)
    channel_name = Column(String, nullable=False)
    uploads_playlist_id = Column(String, nullable=False, unique=True)
    is_active = Column(Boolean, default=True, nullable=False)
    recipe_source = Column(String, default='description', nullable=False)


class Video(Base):
    __tablename__ = 'videos'
    video_id = Column(String, primary_key=True)
    channel_id = Column(String,
                        ForeignKey('channels.channel_id'),
                        nullable=False)
    title = Column(String, nullable=False)
    description = Column(Text)
    published_at = Column(String)
    analysis_status = Column(String, default='pending', nullable=False)
    ai_title = Column(String)
    ai_ingredients = Column(Text)


class SearchLog(Base):
    __tablename__ = 'search_logs'
    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String, index=True)
    keyword = Column(String, index=True)
    channel_id_filter = Column(String, nullable=True)
    result_count = Column(Integer)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class ClickLog(Base):
    __tablename__ = 'click_logs'
    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String, index=True)
    video_id = Column(String, index=True)
    source_section = Column(String)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class PopularIngredient(Base):
    __tablename__ = 'popular_ingredients'
    ingredient_name = Column(String, primary_key=True)
    frequency = Column(Integer, nullable=False)


# --- ▼▼▼ Cloud DB 초기화 (실행부) ▼▼▼ ---
if __name__ == "__main__":
    """
    이 스크립트를 직접 실행(python init_db.py)하면,
    Cloud DB에 연결하여 *존재하지 않는* 테이블만 새로 생성(CREATE)합니다.
    (기존 테이블과 데이터는 보존됩니다.)
    """
    logging.info(f"Cloud DB ({str(engine.url).split('@')[-1]})에 연결 중...")

    try:
        logging.info(
            "DB 스키마 확인 및 새 테이블(예: click_logs) 생성/업데이트 중 (create_all)...")

        # --- drop_all 라인은 데이터 보존을 위해 주석 처리된 상태입니다 ---
        # Base.metadata.drop_all(engine)

        Base.metadata.create_all(engine)  # 테이블 새로 생성 (이미 있으면 건너뜀)

        logging.info("Cloud DB 테이블 스키마 확인/업데이트 완료.")

    except Exception as e:
        logging.error(f"DB 스키마 업데이트 중 오류 발생: {e}")
        logging.error(
            "Neon DB 연결 문자열(DATABASE_URL Secret)이 올바른지, DB가 활성 상태인지 확인하세요.")
# --- ▲▲▲ Cloud DB 초기화 (실행부) ▲▲▲ ---
