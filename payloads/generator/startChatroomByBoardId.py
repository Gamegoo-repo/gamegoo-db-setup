import sys
import os
import random
import json

# 현재 파일 기준으로 최상위 디렉토리를 sys.path에 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from modules import db_fetcher
from modules import random_modules as rm

VUSERS = 300
PAYLOADS_PER_USER = 20
PAYLOAD_NAME = os.path.splitext(os.path.basename(__file__))[0] # 현재 파일 이름에서 확장자 제거

MEMBER_PASSWORD="12345678"

# step 1: member id,email 조회
print(f"[1] Fetching member ids...")
member_emails = [row[0] for row in db_fetcher.fetch_columns("member", ["email"])]

# step 2: board id 조회
print(f"[1] Fetching board ids...")
board_ids = [row[0] for row in db_fetcher.fetch_columns("board", ["board_id"])]

# payload json 생성
file_name = f"{PAYLOAD_NAME}_{VUSERS}vus.json"
file_dir = os.path.join(os.path.dirname(__file__), "../json")
file_path = os.path.join(file_dir, file_name)

print(f"[3] Generating payloads...")

# VUSERS개의 요청을 담을 리스트
payload_data = []

for _ in range(VUSERS):
    payloads = []
    email = random.choice(member_emails)
    for _ in range(PAYLOADS_PER_USER):
        random_board_id = random.choice(board_ids)
        payloads.append({
            "path":{
                "boardId":random_board_id
            }
        })
     
    payload_data.append({
        "email": email,
        "password": MEMBER_PASSWORD,
        "payloads": payloads
    })

# step 4: 파일 저장
with open(file_path, "w", encoding="utf-8") as f:
    json.dump(payload_data, f, indent=2, ensure_ascii=False)

print(f"[4] Payload saved: {file_name}")