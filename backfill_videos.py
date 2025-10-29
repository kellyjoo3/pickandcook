import os
import isodate  # (1단계에서 설치) "PT1M5S" 같은 시간을 초(second)로 변환
import json
import logging
from dotenv import load_dotenv  # (1단계에서 설치) .env 파일 로드
from googleapiclient.discovery import build  # (1단계에서 설치) YouTube API
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from init_db import Channel, Video, engine  # Step 1에서 정의한 DB 모델
from sqlalchemy.orm.exc import NoResultFound

# --- 1. 환경 설정 ---

# .env 파일에서 환경 변수(API 키)를 불러옵니다.
load_dotenv()
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

# --- 로깅 설정 ---
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] - %(message)s')

# --- YouTube API 객체 생성 (logging 사용) ---
if not YOUTUBE_API_KEY:
    # print 대신 logging.error 사용
    logging.error("="*50)
    logging.error(" [오류] .env 파일에 YOUTUBE_API_KEY가 설정되지 않았습니다.")
    logging.error(" .env 파일을 만들고 API 키를 입력하세요.")
    logging.error("="*50)
    exit() # 프로그램 종료

# YouTube API 서비스 빌드
try:
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    logging.info("YouTube API 서비스 빌드 성공.") # 성공 로그 추가 (선택 사항)
except Exception as e:
    # print 대신 logging.error 사용
    logging.error(f" [오류] YouTube API 연결에 실패했습니다: {e}")
    logging.error(" API 키가 유효한지, 인터넷이 연결되어 있는지 확인하세요.")
    exit() # 프로그램 종료
    
# DB 세션 생성
DBSession = sessionmaker(bind=engine)
# session = DBSession()

# --- 2. 헬퍼 함수 정의 ---
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


# --- 3. 메인 함수 정의 ---

def backfill_all_shorts():
    """
    DB에 등록된 채널의 *모든* 영상을 순회하며 쇼츠만 DB에 저장합니다.
    (Pagination - 페이지네이션 처리 포함)
    """
    logging.info("--- [Backfill] 기존 모든 영상 데이터 구축 시작 ---")
    session = DBSession() # 세션 생성 위치 변경 (함수 시작 시)

    try:
        # --- ▼▼▼ 채널 조회 로직 수정 ▼▼▼ ---
        # .first() 대신 .all()을 사용하여 모든 활성 채널 목록을 가져옵니다.
        active_channels = session.query(Channel).filter_by(is_active=True).all() 
        # --- ▲▲▲ 채널 조회 로직 수정 ▲▲▲ ---

        if not active_channels:
            logging.warning("DB에 수집할 활성 채널이 없습니다.")
            # session.close() # finally 블록으로 이동
            return # 함수 종료

        # --- ▼▼▼ 채널별 루프 추가 ▼▼▼ ---
        # 가져온 모든 채널에 대해 반복 작업을 수행합니다.
        for channel in active_channels: 
            logging.info(f"=== 채널 [{channel.channel_name}] 작업 시작 ===") # 채널 구분 로그
            next_page_token = None
            total_processed_in_channel = 0
            total_saved_in_channel = 0

            # --- ▼▼▼ 기존 영상 수집 로직 (while 루프) 시작 - 들여쓰기 추가됨 ▼▼▼ ---
            while True: 
                logging.info(f"  ... API 호출 (페이지 토큰: {next_page_token if next_page_token else '첫 페이지'})")

                # 1. 플레이리스트 아이템(영상 ID) 목록 가져오기
                playlist_request = youtube.playlistItems().list(
                    playlistId=channel.uploads_playlist_id, # 현재 루프의 channel 사용
                    part='contentDetails',
                    maxResults=50,
                    pageToken=next_page_token
                )
                playlist_response = playlist_request.execute()

                video_ids = [item['contentDetails']['videoId'] for item in playlist_response.get('items', [])]
                if not video_ids:
                    logging.info("  ... 이 채널의 모든 페이지 순회 완료.")
                    break # 현재 채널의 while 루프 종료

                logging.info(f"  ... 영상 {len(video_ids)}개 발견. 1개씩 상세 정보 확인...")

                new_videos_batch = []

                # 2. 영상 ID별로 1개씩 상세 정보 요청
                for video_id in video_ids:
                    total_processed_in_channel += 1
                    exists = session.query(Video.video_id).filter_by(video_id=video_id).scalar() is not None
                    if exists: continue

                    try:
                        video_request = youtube.videos().list(
                            part="snippet,contentDetails", id=video_id
                        )
                        video_response = video_request.execute()

                        if not video_response.get('items'): continue
                        video = video_response['items'][0]

                        # 3. (필터) 3분(180초) 이하 영상인지 확인
                        duration_sec = isodate.parse_duration(video['contentDetails']['duration']).total_seconds()

                        if duration_sec <= 180:
                            # 4. 분석 대상 텍스트 추출
                            text_to_analyze = get_text_to_analyze(video_id, video['snippet'], channel.recipe_source)

                            # 5. DB 저장 준비
                            new_video = Video(
                                video_id=video_id,
                                channel_id=channel.channel_id, # 현재 루프의 channel ID 사용
                                title=video['snippet']['title'],
                                description=text_to_analyze,
                                published_at=video['snippet']['publishedAt'],
                                analysis_status='pending'
                            )
                            new_videos_batch.append(new_video)
                            total_saved_in_channel += 1

                    except Exception as e:
                        logging.error(f"    -> ID: {video_id} (상세 정보 처리 중 오류: {e})")

                # 6. 이번 페이지에서 찾은 새 영상들 DB에 저장
                if new_videos_batch:
                    session.add_all(new_videos_batch)
                    session.commit()
                    logging.info(f"    -> [{len(new_videos_batch)}개] 신규 영상 'pending' 상태로 저장 완료")

                # 7. 다음 페이지 토큰 확인
                next_page_token = playlist_response.get('nextPageToken')
                if not next_page_token:
                    logging.info("  ... 이 채널의 모든 페이지 순회 완료.")
                    break # 현재 채널의 while 루프 종료
            # --- ▲▲▲ 기존 영상 수집 로직 (while 루프) 끝 - 들여쓰기 추가됨 ▲▲▲ ---

            logging.info(f"=== 채널 [{channel.channel_name}] 완료. 총 {total_processed_in_channel}개 확인, {total_saved_in_channel}개 신규 저장. ===") # 채널 완료 로그
        # --- ▲▲▲ 채널별 루프 끝 ▲▲▲ ---

        logging.info("--- [Backfill] 모든 활성 채널 작업 완료 ---") # 최종 완료 로그 수정

    except Exception as e:
        session.rollback()
        logging.error(f" [!!! 오류 발생 !!!] 데이터 구축 중 문제 발생: {e}")
    finally:
        if session: # 세션이 생성되었는지 확인 후 닫기
            session.close() # finally 블록에서 세션 닫기

if __name__ == "__main__":
  backfill_all_shorts()