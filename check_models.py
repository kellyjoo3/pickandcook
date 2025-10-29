import google.generativeai as genai
import os
from dotenv import load_dotenv

print("--- Gemini API 모델 목록 진단 시작 ---")

# 1. .env 파일에서 API 키 로드
load_dotenv()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    print("[오류] .env 파일에서 GEMINI_API_KEY를 찾을 수 없습니다.")
    exit()

try:
    # 2. Gemini API 키 설정
    genai.configure(api_key=GEMINI_API_KEY)

    print("API 키를 설정했습니다. 사용 가능한 모델 목록을 조회합니다...")
    print("="*30)
    
    found_models = 0
    
    # 3. "내가 쓸 수 있는 모델 목록"을 API에 요청
    for m in genai.list_models():
        # 4. 우리가 하려는 'generateContent'(텍스트 분석)를 지원하는 모델인지 확인
        if 'generateContent' in m.supported_generation_methods:
            print(f"  -> 사용 가능: {m.name}")
            found_models += 1

    print("="*30)
    
    if found_models == 0:
        print("[진단] 'generateContent'를 지원하는 모델을 찾을 수 없습니다.")
        print("Google Cloud 프로젝트에서 'Generative Language API'가 활성화되었는지 확인하세요.")
    else:
        print(f"[진단 완료] 총 {found_models}개의 사용 가능한 모델을 찾았습니다.")
        print("위 목록(예: 'models/gemini-1.0-pro')에 보이는 이름 중 하나를")
        print("collector.py의 genai.GenerativeModel() 안에 복사해서 사용하세요.")


except Exception as e:
    print(f"\n[!!! 치명적 오류 !!!] API 연결 또는 인증에 실패했습니다.")
    print(f"오류 내용: {e}")
    print("API 키가 정확한지, Google Cloud 프로젝트에서 결제 계정이 활성화되었는지 확인하세요.")
