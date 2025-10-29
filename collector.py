import os
import isodate
import json
import logging
from dotenv import load_dotenv
from googleapiclient.discovery import build
import google.generativeai as genai
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential # Ops: 안정적 재시도를 위함
from init_db import Channel, Video, engine

# --- 1. 기본 설정 (로깅, API 키, DB 세션) ---

# .env 파일에서 모든 환경 변수(API 키 2개) 로드
load_dotenv()
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# 로깅 설정 (Ops: 모니터링을 위함)
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler('collector.log', encoding='utf-8'), # 파일로 저장
        logging.StreamHandler() # 터미널에 출력
    ]
)

# Gemini API 설정
if not GEMINI_API_KEY:
    logging.error("GEMINI_API_KEY가 .env 파일에 없습니다.")
    exit()
genai.configure(api_key=GEMINI_API_KEY)

# YouTube API 설정
if not YOUTUBE_API_KEY:
    logging.error("YOUTUBE_API_KEY가 .env 파일에 없습니다.")
    exit()
try:
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
except Exception as e:
    logging.error(f"YouTube API 연결 실패: {e}")
    exit()
    
# DB 세션 생성
DBSession = sessionmaker(bind=engine)

# --- 2. 헬퍼 함수 (고정 댓글/설명란 텍스트 추출) ---

def get_pinned_comment_text_via_threads(video_id):
    """
    commentThreads().list API를 호출하여 영상의 최상단 댓글 텍스트를 가져옵니다.
    (videos().list 응답에 topLevelComment가 없을 때 사용)
    """
    try:
        # logging.debug(f"  -> {video_id}: topLevelComment 누락 감지. commentThreads API 호출 시도...")
        
        # YouTube API의 commentThreads().list 메서드 호출
        comment_request = youtube.commentThreads().list(
            part='snippet',      # 댓글 내용(snippet)을 가져옵니다.
            videoId=video_id,    # 대상 영상의 ID를 지정합니다.
            maxResults=1,        # 가장 상단의 댓글 스레드 1개만 가져옵니다. 
                                 # (고정 댓글은 보통 맨 위에 있습니다.)
            order='relevance',   # 'relevance'(관련성) 순서로 정렬하면 고정 댓글이 먼저 올 확률이 높지만, 
                                 # 100% 보장되지는 않습니다. 'time' (최신순) 옵션도 고려할 수 있습니다.
            textFormat='plainText' # 댓글 텍스트를 HTML 태그 없이 가져옵니다.
        )
        comment_response = comment_request.execute() # API 호출 실행

        # API 응답에서 댓글 아이템 추출
        items = comment_response.get('items')
        
        if items:
            # 첫 번째 댓글 스레드(items[0])의 snippet 안에 있는 
            # 최상위 댓글(topLevelComment)의 snippet 안의 텍스트(textDisplay)를 찾습니다.
            # 각 단계에서 키가 없을 경우를 대비해 .get()을 안전하게 사용합니다.
            top_comment_snippet = items[0].get('snippet', {}).get('topLevelComment', {}).get('snippet', {})
            comment_text = top_comment_snippet.get('textDisplay')
            
            if comment_text:
                 # 텍스트 추출 성공 로그
                 logging.info(f"  -> {video_id}: commentThreads API로 댓글 텍스트 추출 성공.")
                 return comment_text # 추출된 텍스트 반환
            else:
                 # textDisplay 필드가 비어있는 경우 로그
                 logging.warning(f"  -> {video_id}: commentThreads API 응답에 댓글 텍스트(textDisplay) 없음.")
                 return None # 텍스트 없으면 None 반환
        else:
            # 댓글 스레드 자체가 없는 경우 로그 (댓글이 없거나 비활성화된 영상)
            logging.warning(f"  -> {video_id}: commentThreads API 응답에 댓글 아이템 없음 (댓글 없음?).")
            return None # 아이템 없으면 None 반환
            
    except Exception as e:
        # API 호출 중 발생할 수 있는 모든 오류 처리
        logging.error(f"  -> {video_id}: commentThreads API 호출 중 오류 발생: {e}")
        return None # 오류 발생 시 None 반환

def get_text_to_analyze(video_id, video_snippet, recipe_source):
    """
    DB 설정(recipe_source)에 따라 분석할 텍스트를 결정합니다.
    (최종 해결: topLevelComment 없으면 commentThreads API 호출 후 최종 설명란 fallback)
    """
    # logging.debug(f"  -> {video_id}: get_text_to_analyze 시작. recipe_source='{recipe_source}'") # DEBUG 로그 (필요 시 활성화)
    
    text_result = '' # 기본값은 빈 문자열

    # --- 1. recipe_source가 'pinned_comment'일 경우 ---
    if recipe_source == 'pinned_comment':
        # logging.debug(f"  -> {video_id}: 'pinned_comment' 로직 진입.") # DEBUG 로그
        pinned_comment_obj = video_snippet.get('topLevelComment') # videos().list 응답에서 먼저 찾아봄

        # --- 1a. videos().list 응답에 topLevelComment가 있는 경우 ---
        if pinned_comment_obj:
            # logging.debug(f"  -> {video_id}: 'topLevelComment' 필드 발견!") # DEBUG 로그
            comment_snippet = pinned_comment_obj.get('snippet')
            if comment_snippet:
                extracted_text = comment_snippet.get('textDisplay')
                if extracted_text:
                    logging.info(f"  -> {video_id}: 'topLevelComment' 필드에서 텍스트 추출 성공.")
                    text_result = extracted_text
                else:
                    # textDisplay가 비어있는 경우 -> 추가 API 호출 시도
                    logging.warning(f"  -> {video_id}: 'topLevelComment'는 있으나 'textDisplay' 필드 비어있음. commentThreads API 시도.")
                    text_result = get_pinned_comment_text_via_threads(video_id) # ★ 추가 API 호출 ★
            else:
                 # 내부 snippet 필드가 없는 경우 -> 추가 API 호출 시도
                 logging.warning(f"  -> {video_id}: 'topLevelComment'는 있으나 내부 'snippet' 필드 없음. commentThreads API 시도.")
                 text_result = get_pinned_comment_text_via_threads(video_id) # ★ 추가 API 호출 ★
        
        # --- 1b. videos().list 응답에 topLevelComment가 없는 경우 ---
        else:
            # topLevelComment 필드 자체가 없는 경우 -> 추가 API 호출 시도
            logging.warning(f"  -> {video_id}: API 응답에 'topLevelComment' 필드 없음. commentThreads API 시도.")
            text_result = get_pinned_comment_text_via_threads(video_id) # ★ 추가 API 호출 ★
            
        # --- 1c. 추가 API 호출 후에도 결과가 없다면 최종적으로 '설명란' 사용 ---
        if not text_result: # text_result가 None이거나 빈 문자열('')인 경우
             logging.warning(f"  -> {video_id}: 고정 댓글 최종 추출 실패. 차선책으로 '설명란' 사용 시도.")
             text_result = video_snippet.get('description', '') # 최후의 수단: 설명란
             # logging.debug(f"  -> {video_id}: 설명란 내용:\n--- START ---\n{text_result}\n--- END ---") # DEBUG 로그

    # --- 2. recipe_source가 'description'일 경우 ---
    else: 
        logging.info(f"  -> {video_id}: '설명란'에서 텍스트 추출 시도.")
        text_result = video_snippet.get('description', '')
        # logging.debug(f"  -> {video_id}: 설명란 내용:\n--- START ---\n{text_result}\n--- END ---") # DEBUG 로그

    # --- 3. 최종 반환값 처리 ---
    # 결과가 None일 경우 빈 문자열('')로 통일하여 반환 (AI 분석 함수가 빈 문자열을 처리하도록)
    final_text = text_result if text_result is not None else ''
    # logging.debug(f"  -> {video_id}: get_text_to_analyze 종료. 반환값:\n--- START ---\n{final_text}\n--- END ---") # DEBUG 로그
    return final_text


# --- 3. Gemini AI 분석 함수 (Ops: 안정성 핵심) ---

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def analyze_recipe_with_gemini(text_to_analyze):
    """
    Gemini API를 호출하여 텍스트를 분석합니다.
    Ops 관점: 'tenacity'를 사용해 3회 재시도(Retry) 및 지수 백오프(wait_exponential)를 적용합니다.
    """
    try:
        logging.info("  -> Gemini API 호출 시작...")
        
        # (★매우 중요★)
        # `check_models.py`로 찾은 본인의 모델 이름을 정확히 입력하세요.
        # (예: 'models/gemini-1.0-pro')
        model = genai.GenerativeModel('models/gemini-2.5-flash-lite') 
        
        # [수정됨] V24 - 종료 조건 명시적 리스트 프롬프트
        prompt = f"""
        당신은 요리 레시피 텍스트 분석 AI입니다.
        당신의 임무는 아래 "--- 분석할 텍스트 ---"에서 '주요 재료', '소스 재료' '요리 제목' 3가지 항목을 **명시된 라벨 규칙에 따라 정확히** 추출하여 JSON 객체로 반환하는 것입니다.

        [작업 로직]
        1.  **무시할 내용:** (V29와 동일) 텍스트 시작/끝의 관련 없는 내용, 팁(📌) 등은 결과에 포함하지 마세요.

        2. **주요 재료(`main`) 목록 생성 (문맥 기반 라벨 검증,종료 패턴):**
            * 텍스트에서 **'재료 라벨 후보' 줄(Line)**을 찾습니다. '재료 라벨 후보'란 "재료" 또는 "재료명" 키워드를 포함하며, 뒤에 콜론(:), 이모지(👉🏻 등), 공백 등이 올 수 있는 줄입니다.
            * **후보 줄을 찾았다면, 그 바로 다음 줄부터 몇 줄을 미리 살펴보세요.**
                * **[검증 조건] 만약 다음 줄들이 짧은 명사 형태의 목록(예: "돼지고기 200g", "양파 반 개", "- 대파 1대")으로 보인다면:**
                    * 해당 후보 줄을 **'진짜 재료 라벨'**로 확정합니다.
                    * 짧은 명사 형태의 목록이란, 문장 내 종결어미(요, 다, 네 등)가 없음, 쉼표(,) 또는 줄바꿈으로 구분된 나열형 구조
                    * **진짜 재료 라벨 줄을 찾았다면:**
                        * 그 줄의 **바로 다음 줄**부터 읽기 시작합니다.
                        * **다음 중 하나에 해당하는 '종료 신호' 줄(Line)이 나타나기 직전까지** 읽은 **모든 텍스트 줄(Line)**을 그대로 `main_list`에 저장합니다:
                            * **종료 신호 (라벨):** "소스", "소스👉🏻", "소스 👉🏻", "OO소스:", "양념:", "양념👉🏻", "토핑:", "토핑👉🏻", "레시피", "만드는 법", "팁", "레시피출처" (띄어쓰기 및 합성어 변화 가능성 고려)
                            * **종료 신호 (기호):** 📌,📍, ✅ (해당 기호 및 이모지로 줄을 시작할 경우)
                            * **종료 신호 (형식):** 빈 줄 (empty line)
                            * **종료 신호 (위치):** 텍스트의 끝 (End of text)
                * **'진짜 재료 라벨' 줄을 찾지 못했다면:** `main_list`는 빈 리스트 `[]` 입니다.

        3.  **소스 재료(`sauce`) 목록 생성 (문맥 기반 라벨 검증, 종료 패턴):**
            * 텍스트에서 **'소스 라벨 후보' 줄(Line)**을 찾습니다. '소스 라벨 후보'란, "소스", "양념", "토핑" 키워드로 시작하거나 "OO소스" 형태를 포함하며, 뒤에 콜론, 이모지 등이 올 수 있는 줄입니다.
            * **후보 줄을 찾았다면, 그 바로 다음 줄부터 몇 줄을 미리 살펴보세요.**
                * **[검증 조건] 만약 다음 줄들이 짧은 명사 형태의 목록(예: "돼지고기 200g", "양파 반 개", "- 대파 1대")으로 보인다면:**
                    * 해당 후보 줄을 **'진짜 소스 라벨'**로 확정합니다.
                    * 짧은 명사 형태의 목록이란, 문장 내 종결어미(요, 다, 네 등)가 없음, 쉼표(,) 또는 줄바꿈으로 구분된 나열형 구조
                    * **진짜 소스 라벨 줄을 찾았다면:**
                        * 그 줄의 **바로 다음 줄**부터 읽기 시작합니다.
                        * **다음 중 하나에 해당하는 '종료 신호' 줄(Line)이 나타나기 직전까지** 읽은 **모든 텍스트 줄(Line)**을 그대로 `sauce_list`에 저장합니다:
                            * **종료 신호 (라벨):** "재료", "재료 ", "재료:", "재료👉🏻", "재료 👉🏻", "레시피", "만드는 법:", "팁", "레시피출처" (띄어쓰기 및 합성어 변화 가능성 고려)
                            * **종료 신호 (기호):** 📌,📍, ✅ (해당 기호 및 이모지로 줄을 시작할 경우)
                            * **종료 신호 (형식):** 빈 줄 (empty line)
                            * **종료 신호 (위치):** 텍스트의 끝 (End of text)

                * **'진짜 소스 라벨' 줄을 찾지 못했다면:** `sauce_list`는 빈 리스트 `[]` 입니다.

        4.  **실패 판정 및 실패 JSON 반환 (★★★ 반드시 지시를 수행해야 함 ★★★):**
             * 위 2단계와 3단계를 수행한 결과, `main_list`가 **비어있고 (AND)** `sauce_list`도 **비어있는 경우**:
                        * **반드시** 아래와 **정확히 동일한** JSON 객체를 반환하고 **이후의 제목 추론은 수행하지 않습니다! `title` 값에 다른 문구를 **절대** 넣지 마세요.
                        ```json
                        {{
                        "title": "분석 실패",
                        "main": [],
                        "sauce": []
                        }}
                        ```
        5.  **성공 시 제목 추론 (★★★ 조건부 실행 ★★★):**
            * **오직 Step 4의 실패 조건에 해당하지 않는 경우(즉, `main_list`또는 `sauce_list` 둘 중 하나라도 내용이 있는 경우)에만** 이 단계를 실행합니다.
            * 이제 **"--- 분석할 텍스트 ---" 전체**를 다시 읽고, 추출된 목록을 참고하여 가장 적절한 **'요리 제목'** 1개를 추론하여 `generated_title` 변수에 저장합니다.
            * 이때, `generated_title`은 **텍스트 기반으로 추론된 실제 요리 제목이어야 하며**, "제목 없음", "정보 없음", "알 수 없음" 등과 같은 **실패나 오류를 암시하는 문구를 절대 사용해서는 안 됩니다.**
           
        6.  **성공 시 최종 JSON 생성★:**
            * (Step 5가 실행된 경우) 다음 형식으로 최종 JSON 객체를 생성하여 반환합니다.
                ```json
                {{
                "title": "[추론된 generated_title]",
                "main": main_list,
                "sauce": sauce_list
                }}
                ```

        --- 분석할 텍스트 ---
        {text_to_analyze}
        --- 텍스트 끝 ---

        JSON 형식으로만 응답:
        """      
        
        
        response = model.generate_content(prompt)
        
        # Gemini 응답에서 JSON 부분만 깔끔하게 추출
        cleaned_response = response.text.strip().replace("```json", "").replace("```", "")
        
        logging.info("  -> Gemini API 응답 성공.")
        return json.loads(cleaned_response) # JSON 객체로 변환하여 반환
        
    except Exception as e:
        logging.warning(f"  -> Gemini API 호출 실패 (재시도 예정...): {e}")
        raise e # tenacity가 이 예외를 감지하고 재시도함

# --- 4. 신규 영상 수집 함수 (버그 수정됨) ---

def fetch_new_videos(session):
    """
    DB에 등록된 채널의 *최신* 영상 10개를 확인하여 신규 영상만 'pending'으로 추가합니다.
    (버그 수정: 1-by-1으로 호출하여 고정 댓글 확보)
    """
    logging.info("[A. 신규 영상 수집 작업 시작]")
    try:
        channels = session.query(Channel).filter_by(is_active=True).all()
        if not channels:
            logging.warning("  -> 수집 대상 채널이 DB에 없습니다.")
            return

        for channel in channels:
            logging.info(f"  -> 채널 [{channel.channel_name}] (소스: {channel.recipe_source}) 확인 중...")
            
            # 1. (API 호출 1) 최신 영상 10개의 ID 목록만 가져옴
            playlist_request = youtube.playlistItems().list(
                playlistId=channel.uploads_playlist_id,
                part='contentDetails',
                maxResults=10 
            )
            playlist_response = playlist_request.execute()
            video_ids = [item['contentDetails']['videoId'] for item in playlist_response.get('items', [])]

            if not video_ids:
                logging.info("  -> 새로운 영상이 없습니다.")
                continue

            logging.info(f"  -> 최신 영상 {len(video_ids)}개 발견. 1개씩 상세 정보 확인...")

            # 2. 10개를 1개씩 루프로 호출
            for video_id in video_ids:
                
                # 3. (중복 방지)
                exists = session.query(Video).filter_by(video_id=video_id).first()
                if exists:
                    logging.info(f"  -> ID: {video_id} (이미 DB에 존재함. 건너뛰기)")
                    continue
                
                # 4. (API 호출 2) 1개의 영상 ID로 상세 정보 요청 (고정 댓글을 위해 필수)
                try:
                    video_request = youtube.videos().list(
                        part="snippet,contentDetails",
                        id=video_id
                    )
                    video_response = video_request.execute()
                    
                    if not video_response.get('items'):
                        logging.warning(f"  -> ID: {video_id} (상세 정보 API 호출 실패. 건너뛰기)")
                        continue
                        
                    video = video_response['items'][0]

                    # 5. (필터) 3분(180초) 이하인지 확인
                    duration_sec = isodate.parse_duration(video['contentDetails']['duration']).total_seconds()
                    
                    if duration_sec <= 180:
                        if duration_sec <= 180:
                            logging.info(f"  -> ID: {video_id} (신규 180초 이하 영상 발견!)")

                            # [ ★ 진단 로그 추가 1: 입력값 확인 ★ ]
                            snippet_data = video.get('snippet', {})
                            logging.debug(f"  -> get_text_to_analyze 입력값 (ID: {video_id}, 소스: {channel.recipe_source}):\n{json.dumps(snippet_data, indent=2, ensure_ascii=False)}")

                            # 6. (데이터 추출) 설정에 따라 텍스트 추출
                            text_to_analyze = get_text_to_analyze(snippet_data, channel.recipe_source)

                            # [ ★ 진단 로그 추가 2: 출력값 확인 ★ ]
                            logging.debug(f"  -> get_text_to_analyze 출력값 (ID: {video_id}):\n--- START ---\n{text_to_analyze}\n--- END ---")

                            # 7. (DB 저장)
                            new_video = Video(
                                video_id=video_id,
                                channel_id=channel.channel_id,
                                title=snippet_data.get('title', '제목 없음'), # snippet_data 사용
                                description=text_to_analyze, # <- 여기에 빈 값이 들어가는지 확인
                                published_at=snippet_data.get('publishedAt'), # snippet_data 사용
                                analysis_status='pending'
                            )
                            session.add(new_video)
                            session.commit()
                        
                    
                except Exception as e:
                    # (이 except는 1개 영상 ID 호출 실패 시)
                    logging.error(f"  -> ID: {video_id} (상세 정보 API 호출 실패: {e})")
                    session.rollback()
            
            logging.info(f"  -> 채널 [{channel.channel_name}] 신규 영상 확인 완료.")

    # (★ 여기입니다 ★) 이 try 블록의 짝꿍인 except
    except Exception as e:
        logging.error(f"[A] 신규 영상 수집 중 오류: {e}")
        session.rollback()

# --- 5. AI 분석 처리 함수 (null 방지 수정됨) ---

def process_pending_videos(session):
    """
    DB에서 'pending' 또는 'failed' 상태인 모든 영상(150개 포함)을 가져와 AI로 분석합니다.
    """
    logging.info("[B. AI 분석 작업 시작]")
    
    # 'pending' 또는 'failed' 상태인 영상 목록을 모두 조회 (재분석을 위함)
    videos_to_process = session.query(Video).filter(
        Video.analysis_status.in_(['pending', 'failed'])
    ).all()
    
    if not videos_to_process:
        logging.info("  -> 분석할 'pending' 또는 'failed' 상태의 영상이 없습니다.")
        return

    logging.info(f"  -> 총 {len(videos_to_process)}개의 영상을 분석(재분석)합니다.")

    # for 루프가 'videos_to_process' 변수를 사용하는지 확인
    for video in videos_to_process:
        logging.info(f"  -> 영상 분석 중: {video.video_id} (상태: {video.analysis_status})...")
        try:
            # [ ★ 진단 로그 추가 ★ ]
            # Gemini에게 보내기 직전의 텍스트를 로그로 남깁니다.
            logging.info(f"  -> 분석 대상 텍스트 (ID: {video.video_id}):\n--- START ---\n{video.description}\n--- END ---")
            
            # 5-1. 3회 재시도 로직이 포함된 Gemini 함수 호출
            analysis_result = analyze_recipe_with_gemini(video.description)
            
            # 5-2. 분석 결과 기본값 설정
            # Gemini가 반환한 전체 JSON 객체를 가져옵니다 (기본값은 실패 형태).
            result_json = analysis_result or {"title": "분석 실패", "main": [], "sauce": []}

            # --- ▼▼▼ Python 후처리 코드 추가 위치 ▼▼▼ ---
            # [ ★ Python 후처리 시작 ★ ]
            # AI가 반환한 main/sauce 리스트가 실제로 비어있는지 확인
            main_list_empty = not result_json.get('main') # 비어있으면 True
            sauce_list_empty = not result_json.get('sauce') # 비어있으면 True

            # 만약 두 리스트가 모두 비었는데 title이 '분석 실패'가 아니라면, 강제로 수정
            if main_list_empty and sauce_list_empty and result_json.get('title') != "분석 실패":
                logging.warning(f"  -> ID: {video.video_id}: AI가 실패 조건을 무시하고 제목 생성 ('{result_json.get('title')}'). '분석 실패'로 강제 수정됨.")
                result_json['title'] = "분석 실패" # 파이썬에서 강제로 덮어쓰기
            # [ ★ Python 후처리 끝 ★ ]
            # --- ▲▲▲ Python 후처리 코드 추가 위치 ▲▲▲ ---

            # 5-3. 최종 검증된 결과로 DB 업데이트
            # ai_title 컬럼에는 최종 결정된 'title' 값만 저장합니다.
            video.ai_title = result_json.get('title') # 이제 '분석 실패' 또는 실제 제목

            ## ai_ingredients 컬럼에는 전체 JSON 객체를 '문자열'로 저장합니다.
            # [ ★ 수정됨: 한글 인코딩 문제 해결 ★ ]
            video.ai_ingredients = json.dumps(result_json, ensure_ascii=False) 

            # ai_recipe_steps 관련 코드는 모두 삭제합니다.

            video.analysis_status = 'completed'
            # 로그 메시지를 최종 결과 반영하도록 수정
            logging.info(f"  -> 분석 최종 결과: {video.ai_title}")
            

        except Exception as e:
            # 5-3. (★ null 방지 수정됨 ★)
            logging.error(f"  -> [AI 통신 오류] 영상 {video.video_id} 분석 최종 실패: {e}")
            video.analysis_status = 'failed' # '실패' 상태로 변경
            
            ## [ ★ null 방지 코드 추가 ★ ]
            # video.ai_title = "AI 통신 오류" 
            # video.ai_ingredients = "[]"
            # video.ai_recipe_steps = "[]"
            
            video.ai_title = "AI 통신 오류"
            # ai_ingredients에 실패 JSON 문자열 저장
            # [ ★ 수정됨: 한글 인코딩 문제 해결 ★ ]
            video.ai_ingredients = json.dumps({"title": "AI 통신 오류", "main": [], "sauce": []}, ensure_ascii=False)
            #video.ai_ingredients = json.dumps({"title": "AI 통신 오류", "main": [], "sauce": []})
            # ai_recipe_steps 관련 코드는 모두 삭제합니다.
        
        session.commit() # 각 영상 처리 후 즉시 커밋

    logging.info("[B] AI 분석 작업 완료.")

# --- 6. 메인 실행부 (이전과 동일) ---

if __name__ == "__main__":
    """
    이 스K크립트(collector.py)가 실행되면,
    1. (미래를 위해) 새로운 영상이 있는지 확인하고,
    2. (과거와 현재를 위해) 'pending' 또는 'failed' 영상을 분석합니다.
    """
    session = DBSession()
    logging.info("="*50)
    logging.info("--- [Collector] 작업 시작 (v5. 최종본) ---")
    
    try:
        # A. 신규 영상 수집
        fetch_new_videos(session)
        
        # B. 'pending'/'failed' 영상 분석
        process_pending_videos(session)
        
    except SQLAlchemyError as e:
        logging.error(f"DB 오류 발생: {e}")
        session.rollback()
    except Exception as e:
        logging.error(f"알 수 없는 오류 발생: {e}")
    finally:
        session.close() # (★ 여기입니다 ★) 이 try...finally가 메인 실행부의 짝꿍
        logging.info("--- [Collector] 작업 완료 ---")
        logging.info("="*50)

