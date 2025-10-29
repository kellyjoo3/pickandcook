import sqlalchemy
from sqlalchemy import create_engine, Column, String, Boolean, Text, ForeignKey, Integer, DateTime, func
from sqlalchemy.orm import sessionmaker, declarative_base
import os

# --- 이 스크립트의 핵심 설정 ---
# 1. 앞으로 우리가 만들 데이터베이스 파일의 이름입니다.
DB_FILENAME = 'recipes.db'
# ------------------------------

# 2. 데이터베이스 엔진 생성: "이 스크립트가 있는 폴더에 recipes.db라는 파일을 만들어줘"
#    'sqlite:///'는 파일 기반 DB를 의미합니다.
engine = create_engine(f'sqlite:///{DB_FILENAME}')

# 3. SQLAlchemy의 기본 설정 (그대로 사용하면 됩니다)
Base = declarative_base()


# 4. 첫 번째 테이블(시트) 설계: 'channels'
class Channel(Base):
    """
    수집할 유튜버 채널 정보를 저장하는 테이블 (확장성 담당)
    """
    __tablename__ = 'channels'  # 테이블의 실제 이름

    # --- 컬럼 (엑셀의 열) 정의 ---
    channel_id = Column(String, primary_key=True)  # 채널 ID (기본 키)
    channel_name = Column(String, nullable=False)  # 채널 이름
    uploads_playlist_id = Column(String, nullable=False,
                                 unique=True)  # 수집할 영상 목록 ID
    is_active = Column(Boolean, default=True, nullable=False)  # 수집 활성화 스위치

    # Ops 관점: 이 채널의 레시피를 어디서 가져올지 (설명란 or 고정댓글)
    recipe_source = Column(String, default='description', nullable=False)


class Video(Base):
    """
    수집된 개별 영상 정보를 저장하는 테이블
    """
    __tablename__ = 'videos'

    video_id = Column(String, primary_key=True)  # 영상 ID (기본 키)
    # 'channels' 테이블의 'channel_id'와 연결되는 외래 키
    channel_id = Column(String,
                        ForeignKey('channels.channel_id'),
                        nullable=False)
    title = Column(String, nullable=False)

    # 원본 텍스트 (설명란 또는 고정댓글에서 가져옴)
    description = Column(Text)
    published_at = Column(String)

    # Ops 관점: AI 분석 상태 관리
    analysis_status = Column(
        String, default='pending',
        nullable=False)  # 'pending', 'completed', 'failed'

    # AI 분석 결과
    ai_title = Column(String)
    ai_ingredients = Column(Text)  # <- 여기에 통합 JSON 문자열 저장 예정
    # ai_recipe_steps = Column(Text) # <- 삭제 (추출 복잡함. ingredients만 출력으로 변경)


class SearchLog(Base):
    """
    사용자 검색 키워드를 저장하는 테이블
    """
    __tablename__ = 'search_logs'

    id = Column(Integer, primary_key=True, index=True)
    keyword = Column(String, index=True)
    channel_id_filter = Column(String, nullable=True)
    result_count = Column(Integer)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


# --- ▲▲▲ 검색 로그 테이블 추가 ▲▲▲ ---

# [ ★ 인기 재료 테이블 추가 ★ ] 26
#class PopularIngredient(Base):
#    """
#    자주 등장하는 상위 재료 목록을 저장하는 테이블
#    """
#    __tablename__ = 'popular_ingredients'
#
#    ingredient_name = Column(String, primary_key=True) # 재료 이름 (기본 키)
#    frequency = Column(Integer, nullable=False)      # 등장 빈도

# 5. 이 스크립트(init_db.py)를 직접 실행했을 때만 동작하는 부분
if __name__ == "__main__":
    print(f"데이터베이스 파일 '{DB_FILENAME}'과 테이블 생성을 시작합니다...")

    # 6. 위에서 설계한(Channel, Video) 모든 테이블을 'engine'에 실제로 생성합니다.
    Base.metadata.create_all(engine)

    print(f"'{DB_FILENAME}' 파일이 생성되고 테이블이 성공적으로 정의되었습니다.")
    print("VS Code 왼쪽 탐색기에서 'recipes.db' 파일이 생겼는지 확인하세요.")
