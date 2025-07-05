import sys
import os
import random
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from modules import db_fetcher
from modules import random_modules as rm
from modules import enums

VUSERS = 70
PAYLOADS_PER_USER = 20
PAYLOAD_NAME = os.path.splitext(os.path.basename(__file__))[0] # 현재 파일 이름에서 확장자 제거

MEMBER_PASSWORD="12345678"

# step 1: member 조회
print(f"[1] Fetching member...")
member_emails = [row[0] for row in db_fetcher.fetch_columns("member", ["email"])]

# payload json 생성
file_name = f"{PAYLOAD_NAME}_{VUSERS}vus.json"
file_dir = os.path.join(os.path.dirname(__file__), "../json")
file_path = os.path.join(file_dir, file_name)

print(f"[2] Generating payloads...")
# VUSERS개의 요청을 담을 리스트
payload_data = []
start = "2025-06-04T00:00:00"

for _ in range(VUSERS):
    payloads = []
    for _ in range(PAYLOADS_PER_USER):
        payloads.append({
            "query": {
                "cursor": rm.random_iso8601_datetime(start),
                "gameMode":random.choice(enums.GAMEMODE),
                "tier":random.choice(enums.TIER),
                "mainP":random.choice(enums.POSITION)
            }
        })
    
    payload_data.append({
        "email":random.choice(member_emails),
        "password":MEMBER_PASSWORD,
        "payloads": payloads
    })

# step 3: 파일 저장
with open(file_path, "w", encoding="utf-8") as f:
    json.dump(payload_data, f, indent=2, ensure_ascii=False)

print(f"[3] Payload saved: {file_name}")