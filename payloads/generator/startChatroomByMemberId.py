import sys
import os
import random
import json

# 현재 파일 기준으로 최상위 디렉토리를 sys.path에 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from modules import db_fetcher
from modules import random_modules as rm

VUSERS = 400
PAYLOADS_PER_USER = 20
PAYLOAD_NAME = os.path.splitext(os.path.basename(__file__))[0] # 현재 파일 이름에서 확장자 제거

MEMBER_PASSWORD="12345678"

# step 1: member id,email 조회
print(f"[1] Fetching member ids...")
members = db_fetcher.fetch_columns("member", ["member_id","email"]) # [member_id,email]

member_ids = list(row[0] for row in members)
member_emails = list(row[1] for row in members)

# payload json 생성
file_name = f"{PAYLOAD_NAME}_{VUSERS}vus.json"
file_dir = os.path.join(os.path.dirname(__file__), "../json")
file_path = os.path.join(file_dir, file_name)

print(f"[2] Generating payloads...")

# VUSERS개의 요청을 담을 리스트
payload_data = []

for _ in range(VUSERS):
    payloads = []
    i = rm.sample_integer(1,len(members)-1)
    while len(payloads) < PAYLOADS_PER_USER:
        random_member_id = random.choice(member_ids)
        if random_member_id != member_ids[i]:
            payloads.append({
                "path": {
                    "memberId": random_member_id
                }
        })
            
    payload_data.append({
        "email": member_emails[i],
        "password": MEMBER_PASSWORD,
        "payloads": payloads
    })

# step 3: 파일 저장
with open(file_path, "w", encoding="utf-8") as f:
    json.dump(payload_data, f, indent=2, ensure_ascii=False)

print(f"[3] Payload saved: {file_name}")