import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from init_db import SearchLog  # search_logs 테이블 모델 가져오기
from database import engine  # database.py의 engine 가져오기

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] [%(levelname)s] - %(message)s')


def check_search_logs():
    Session = sessionmaker(bind=engine)
    session = Session()
    logging.info("--- search_logs 테이블 조회 시작 ---")
    try:
        # search_logs 테이블의 모든 데이터를 조회
        logs = session.query(SearchLog).all()

        if not logs:
            logging.warning("!!! 조회 결과: search_logs 테이블이 비어있습니다. !!!")
        else:
            logging.info(f"--- 총 {len(logs)}개의 검색 로그 발견! ---")
            # 최근 5개 로그만 출력 (예시)
            for log in logs[-5:]:
                print(
                    f"  [Log ID: {log.id}] Keyword: '{log.keyword}', Channel: '{log.channel_id_filter}', Count: {log.result_count}, Time: {log.timestamp}"
                )
            logging.info("---------------------------------------")

    except Exception as e:
        logging.error(f"DB 조회 중 오류 발생: {e}")
    finally:
        session.close()
        logging.info("--- 조회 완료 ---")


if __name__ == "__main__":
    check_search_logs()
