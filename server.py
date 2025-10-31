import json
import logging
from fastapi import FastAPI, Request, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles  # (Optional) If you have CSS/JS files later
from pydantic import BaseModel
from sqlalchemy.orm import Session
# [ ★ 수정됨: create_engine, text 제거 ★ ] 26
from sqlalchemy import create_engine, func, text, or_, and_  # v1.1 2개 이상 검색어(and_추가)
from database import SessionLocal
from init_db import Video, Channel, SearchLog, ClickLog  # Import DB models

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] [%(levelname)s] - %(message)s')
# FastAPI 앱 생성
app = FastAPI()


class ClickLogRequest(BaseModel):  # ★ 이 클래스 정의가 있는지? ★
    session_id: str
    video_id: str
    source_section: str


# --- 1. 기본 설정 (로깅, DB 세션) ---

logging.basicConfig(
    level=logging.INFO,  # 운영 시 INFO 레벨 사용
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler('api.log', encoding='utf-8'),  # API 로그 파일
        logging.StreamHandler()
    ])

# DB 연결 (init_db.py와 동일한 설정 사용)
engine = create_engine('sqlite:///recipes.db')

#DBSession = sessionmaker(bind=engine)

# --- (Optional) 정적 파일 서빙 설정 ---
# app.mount("/static", StaticFiles(directory="static"), name="static")


# --- 2. 헬퍼 함수 (DB 세션 관리) ---
def get_db():
    """DB 세션 생성 함수 (FastAPI 의존성 주입용)"""
    #db = DBSession()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- 3. API 엔드포인트 정의 ---


@app.get("/", response_class=HTMLResponse)
async def read_root():
    """메인 HTML 페이지를 반환합니다."""
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content, status_code=200)
    except FileNotFoundError:
        logging.error("index.html 파일을 찾을 수 없습니다.")
        raise HTTPException(status_code=404, detail="index.html not found")
    except Exception as e:
        logging.error(f"HTML 파일 읽기 오류: {e}")
        raise HTTPException(status_code=500, detail="Error reading HTML file")


@app.get("/api/channels", response_class=JSONResponse)
#async def get_channels():
def get_channels(db: Session = Depends(get_db)):
    logging.info("API: /api/channels 호출됨")
    """활성화된 채널 목록을 반환합니다."""
    # db = DBSession()
    try:
        active_channels = db.query(Channel.channel_id, Channel.channel_name)\
                            .filter(Channel.is_active == True)\
                            .order_by(Channel.channel_name)\
                            .all()
        #channels = db.query(Channel).filter(Channel.is_active == True).all()
        # SQLAlchemy 객체를 딕셔너리로 변환
        #return [{"channel_id": c.channel_id, "channel_name": c.channel_name} for c in channels]
        channel_list = [{
            "id": ch_id,
            "name": name
        } for ch_id, name in active_channels]
        logging.info(f"API: /api/channels - {len(channel_list)}개 채널 반환")
        return channel_list
    except Exception as e:
        logging.error(f"채널 목록 조회 오류: {e}")
        raise HTTPException(status_code=500, detail="Database query error")
    #finally:
    #db.close()


@app.get("/api/recommendations", response_class=JSONResponse)
# --- ▼▼▼ Depends(get_db) 추가 ▼▼▼ ---
async def get_recommendations(db: Session = Depends(get_db)):
    # --- ▲▲▲ Depends(get_db) 추가 ▲▲▲ ---
    """분석 완료된 레시피 중 랜덤 3개를 추천합니다."""
    # db = DBSession() # <- ★★★ 이 줄 삭제 ★★★
    logging.info("API: /api/recommendations 호출됨")
    try:
        # 이제 매개변수로 받은 'db' 변수를 사용
        random_videos = db.query(
        Video.video_id,
        Video.ai_title.label("title"),
        Video.ai_ingredients,
        Channel.channel_name # ★★★ 채널 이름 조회 ★★★
        ).join(Channel, Video.channel_id == Channel.channel_id)\
     .filter( # filter는 join 다음에
        Video.analysis_status == 'completed',
        Video.ai_title != '분석 실패',
        Video.ai_title != 'AI 통신 오류'
        ).order_by(func.random()).limit(3).all()

        # ★★★ 결과 변환 시 channel_name 포함 ★★★
        recipe_list = [{
            "video_id": r.video_id,
            "title": r.title,
            "ai_ingredients": r.ai_ingredients,
            "channel_name": r.channel_name
        } for r in random_videos]
        logging.info(f"API: /api/recommendations - {len(recipe_list)}개 결과 반환")
        return recipe_list
    except Exception as e:
        logging.error(f"Error fetching recommendations: {e}")
        raise HTTPException(status_code=500, detail="추천 목록 조회 중 오류 발생")
    #finally:
    #db.close()


#@app.get("/api/search")
#async def search_recipes(
#    keyword: str = Query(None, min_length=1, description="검색할 키워드 (제목 또는 재료)"),
#    channel_id: str | None = Query(None, description="특정 채널 ID (선택 사항)")
#):


# [ ★ 수정된 최종 함수 정의 ★ ]
@app.get("/api/search", response_class=JSONResponse)  # response_class 추가
async def search_recipes(
        # 기존 Query 파라미터 유지
        keyword: str | None = Query(None,
                                    min_length=1,
                                    description="검색할 키워드 (제목 또는 재료)"),
        channel_id: str | None = Query(None, description="특정 채널 ID (선택 사항)"),
        session_id: str | None = Query(None, description="사용자 세션 ID"),
        # DB 세션 의존성 추가 (필수)
        db: Session = Depends(get_db)):
    """ai_title 또는 ai_ingredients에서 키워드로 레시피를 검색합니다. (채널 필터링 추가)"""
    logging.info(
        f"API: /api/search 호출됨 (keyword='{keyword}', channel_id='{channel_id}', session_id='{session_id}')"
    )
    try:
        # --- ▼▼▼ 함수 본문 (try 블록 내부) ▼▼▼ ---
        # 이 아래의 'query = db.query(...)' 부터 'return recipe_list' 까지의
        # 함수 *내용*은 이전에 제가 제안했던 'or_' 및 'contains()'를 사용하는
        # 코드로 교체하는 것이 맞습니다.

        # 기본 쿼리 설정
        query = db.query(
        Video.video_id,
        Video.ai_title.label("title"),
        Video.ai_ingredients,
        Channel.channel_name # ★★★ 채널 이름 조회 ★★★
        ).join(Channel, Video.channel_id == Channel.channel_id)\
     .filter( # filter는 join 다음에
        Video.analysis_status == 'completed',
        Video.ai_title != '분석 실패',
        Video.ai_title != 'AI 통신 오류'
        )

        # 채널 필터링
        if channel_id:
            logging.debug(f"  -> 채널 필터 적용: {channel_id}")
            query = query.filter(Video.channel_id == channel_id)

        # 키워드 필터링 (★ 동적 AND 로직으로 수정 ★) # v1.1 2개 이상 검색어
        if keyword:
            logging.debug(f"  -> 키워드 필터 적용 (AND 검색): {keyword}")

            # 1. 검색어를 공백 기준으로 분리 (예: "김치 고기" -> ["김치", "고기"])
            search_terms = keyword.split()

            # 2. "AND"로 묶을 조건 리스트 생성 (예: [ (김치조건), (고기조건) ])
            and_conditions = []

            # 3. 각 단어(term)별로 (제목 OR 재료) 조건을 만듭니다
            for term in search_terms:
                # (제목에 '김치'가 있거나 OR 재료에 '김치'가 있거나)
                or_condition = or_(Video.ai_title.contains(term),
                                   Video.ai_ingredients.contains(term))
                and_conditions.append(or_condition)

            # 4. 모든 조건(... AND ... AND ...)을 쿼리에 최종 적용합니다
            if and_conditions:
                query = query.filter(and_(*and_conditions))

        # 결과 가져오기 및 변환
        results = query.order_by(Video.published_at.desc()).limit(50).all()
        recipe_list = [{
            "video_id": r.video_id,
            "title": r.title,
            "ai_ingredients": r.ai_ingredients,
            "channel_name": r.channel_name
        } for r in results]

        try:
            new_log_entry = SearchLog(session_id=session_id,
                                      keyword=keyword,
                                      channel_id_filter=channel_id,
                                      result_count=len(recipe_list))
            db.add(new_log_entry)
            db.commit()  # ★ 중요: 이 commit은 로그 저장을 위한 것이므로 괜찮습니다.
            logging.info(f"  -> 검색어 '{keyword}' DB에 로깅 완료.")
        except Exception as log_e:
            logging.error(f"  -> 검색어 로깅 실패: {log_e}")
            db.rollback()  # 로그 저장 실패 시 롤백
        # --- ▲▲▲ 검색 로그 DB에 저장 ▲▲▲ ---

        logging.info(f"API: /api/search - {len(recipe_list)}개 결과 반환")
        return recipe_list
        # --- ▲▲▲ 함수 본문 (try 블록 내부) ▲▲▲ ---

    except Exception as e:
        logging.error(f"Error during search: {e}")
        raise HTTPException(status_code=500, detail="검색 중 오류 발생")


# --- [ ★ 여기에 새 API 함수 추가 ★ ] --- 26
# --- API: 등록된 채널 목록 ---
@app.get("/api/channels", response_class=JSONResponse)
def get_channels(db: Session = Depends(get_db)):
    """DB에 등록된 활성 채널 목록 (ID, 이름)을 반환합니다."""
    logging.info("API: /api/channels 호출됨")  # 로그 추가
    try:
        # is_active가 true인 채널만 가져옴
        active_channels = db.query(Channel.channel_id, Channel.channel_name)\
                            .filter(Channel.is_active == True)\
                            .order_by(Channel.channel_name)\
                            .all()

        # 결과를 {"id": ..., "name": ...} 형태의 딕셔너리 리스트로 변환
        channel_list = [{
            "id": ch_id,
            "name": name
        } for ch_id, name in active_channels]
        logging.info(
            f"API: /api/channels - {len(channel_list)}개 채널 반환")  # 로그 추가
        return channel_list
    except Exception as e:
        logging.error(f"Error fetching channels: {e}")  # 오류 로그 강화
        # FastAPI에서는 오류 발생 시 HTTP 상태 코드와 함께 오류 응답을 보내는 것이 좋습니다.
        raise HTTPException(status_code=500, detail="채널 목록 조회 중 오류 발생")


@app.post("/api/log-click")  # ★ 이 함수 전체가 있는지? ★
async def log_click(request: ClickLogRequest, db: Session = Depends(get_db)):
    """썸네일 클릭 이벤트를 DB에 저장합니다."""
    logging.info(f"API: /api/log-click 호출됨 (video_id='{request.video_id}')")
    try:
        new_click = ClickLog(session_id=request.session_id,
                             video_id=request.video_id,
                             source_section=request.source_section)
        db.add(new_click)
        db.commit()
        return {"status": "success"}
    except Exception as e:
        logging.error(f"  -> 클릭 로깅 실패: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="클릭 로그 저장 실패")

    # return [] # 또는 빈 리스트 반환 유지
