from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# 1. 'init_db.py' 파일에서 정의했던 'Channel' 테이블 설계도와 'engine' (DB 연결 통로)을 가져옵니다.

# --- ↓↓↓ 디버그 코드 1 (Import 직후) ↓↓↓ ---
try:
    print(f"DEBUG (add_channel Import 직후): sessionmaker 타입: {type(sessionmaker)}")
    if 'sessionmaker' not in str(type(sessionmaker)).lower():
         print("!!! WARNING (add_channel): Import 직후 sessionmaker 타입 이상 !!!")
except NameError:
    print("!!! ERROR (add_channel): Import 직후 sessionmaker NameError 발생 !!!")
except Exception as e:
    print(f"!!! ERROR (add_channel): Import 직후 타입 확인 중 예외: {e}")
# --- ↑↑↑ 디버그 코드 1 --- ---


from init_db import Channel, engine 

# 2. DB와 대화할 수 있는 '세션(Session)'을 만듭니다.
#DBSession = sessionmaker(bind=engine)
#session = DBSession()


# --- ↓↓↓ 디버그 코드 2 (사용 직전) ↓↓↓ ---
try:
    print(f"DEBUG (add_channel 사용 직전): sessionmaker 타입: {type(sessionmaker)}")
    if 'sessionmaker' not in str(type(sessionmaker)).lower():
         print("!!! WARNING (add_channel): 사용 직전 sessionmaker 타입 이상 !!!")
    # --- 실제 사용 ---
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    print("DEBUG (add_channel): DBSession 및 session 생성 성공.")
except NameError:
    print("!!! CRITICAL ERROR (add_channel): sessionmaker NameError 발생 !!!")
    raise # 오류 발생시켜 프로그램 중지
except Exception as e:
    print(f"!!! CRITICAL ERROR (add_channel): DBSession 생성 중 예외 발생: {e}")
    raise
# --- ↑↑↑ 디버그 코드 2 --- ---



def add_new_channel():
    print("--- 새 유튜버 채널을 DB에 등록합니다 ---")
    
    # 3. try...except 구문: DB 작업 중 오류가 나도 프로그램이 멈추지 않게 합니다.
    try:
        # 4. 터미널에서 사용자에게 직접 정보를 입력받습니다.
        channel_id = input("1. 대상 유튜버의 '채널 ID'를 입력하세요 (예: UC...): ").strip()
        channel_name = input("2. 채널 이름을 입력하세요 (관리용, 예: 백종원): ").strip()
        
        # 'UC...' ID를 'UU...' 업로드 ID로 자동 변환해 줍니다.
        default_uploads_id = f"UU{channel_id[2:]}"
        uploads_id = input(f"3. '업로드 플레이리스트 ID'를 입력하세요 (자동완성: {default_uploads_id}): ").strip()
        
        # 만약 그냥 엔터를 치면 자동완성된 ID를 사용합니다.
        if not uploads_id:
            uploads_id = default_uploads_id
            print(f"   (자동 입력됨: {uploads_id})")

        print("4. 레시피 데이터 소스를 선택하세요. (Ops 핵심 설정)")
        print("   (1) 영상 설명란 (description)")
        print("   (2) 영상 고정 댓글 (pinned_comment)")
        source_choice = input("   선택 (기본값 1): ").strip()

        recipe_source = 'description' # 기본값
        if source_choice == '2':
            recipe_source = 'pinned_comment'
            print("   (소스: '고정 댓글'로 설정)")
        else:
            print("   (소스: '설명란'으로 설정)")

        # 5. 입력받은 정보로 'Channel' 객체(새로운 행)를 만듭니다.
        new_channel = Channel(
            channel_id=channel_id,
            channel_name=channel_name,
            uploads_playlist_id=uploads_id,
            recipe_source=recipe_source,
            is_active=True # 수집 대상으로 즉시 활성화
        )
        
        # 6. 세션을 통해 이 '새로운 행'을 DB에 추가(add)합니다.
        session.add(new_channel)
        
        # 7. '커밋(commit)'을 해야 실제 파일에 최종 저장됩니다.
        session.commit()
        
        print(f"\n[성공] '{channel_name}' 채널을 'channels' 테이블에 성공적으로 등록했습니다.")

    except Exception as e:
        # 8. 만약 channel_id 중복 등 오류가 발생하면, 변경사항을 취소(rollback)합니다.
        session.rollback() 
        print(f"\n[오류] 채널 등록에 실패했습니다: {e}")
        print("채널 ID가 이미 존재하지 않는지 확인하세요.")
    finally:
        # 9. 작업이 성공하든 실패하든, DB와의 연결(세션)을 항상 닫아줍니다.
        session.close() 

# 10. 이 스크립트(add_channel.py)를 직접 실행했을 때 'add_new_channel' 함수를 실행합니다.
if __name__ == "__main__":
    add_new_channel()