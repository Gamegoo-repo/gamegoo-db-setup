import sys
import os
import random
import csv
import json
from collections import defaultdict
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from modules import db_fetcher
from modules import random_modules as rm

PAYLOAD_NAME = os.path.splitext(os.path.basename(__file__))[0] # 현재 파일 이름에서 확장자 제거

input_csv_path = "email_chatroomUuid.csv"
# 이메일별 UUID 저장용 딕셔너리
email_to_uuids = defaultdict(list)

MEMBER_PASSWORD="12345678"

# step 1: csv 파일 읽기
print(f"[1] Read csv...")
with open(input_csv_path, newline='', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        if len(row) != 2:
            continue  # 유효하지 않은 라인 건너뜀
        email, uuid = row
        email_to_uuids[email].append(uuid)

VUSERS = len(email_to_uuids)

# payload json 생성
file_name = f"{PAYLOAD_NAME}_{VUSERS}vus.json"
file_dir = os.path.join(os.path.dirname(__file__), "../json")
file_path = os.path.join(file_dir, file_name)

print(f"[2] Generating payloads...")

payload_data = []

for email, uuid_list in email_to_uuids.items():
    payloads = [{"path": {"chatroomUuid": uid}} for uid in uuid_list]
    payload_data.append({
        "email": email,
        "password":MEMBER_PASSWORD,
        "payloads": payloads
    })

# step 3: 파일 저장
with open(file_path, "w", encoding="utf-8") as f:
    json.dump(payload_data, f, indent=2, ensure_ascii=False)

print(f"[3] Payload saved: {file_name}")