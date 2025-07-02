import csv
import time
import random
import uuid
from datetime import datetime
from modules import uploader
from modules import db_fetcher
from itertools import combinations
from modules import enums
from modules import random_modules as rm
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

CHATROOM_TABLE = 'chatroom'
CHATROOM_HEADERS = ['uuid', 'last_chat_at','last_chat_id','created_at']

MEMBER_CHATROOM_TABLE = 'member_chatroom'
MEMBER_CHATROOM_HEADERS=['chatroom_id','member_id','last_view_date','last_join_date','created_at']

NULL = '\\N'

# chatroom csv 생성
def generate_chatroom_csv(k):
    sorted_dates = rm.generate_sorted_created_at_list(k,60,30) #  created_at 추출

    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{CHATROOM_TABLE}_{k}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CHATROOM_HEADERS)
        writer.writeheader()
        for created_at in sorted_dates:
            row={ 
                'uuid':uuid.uuid4(),
                'last_chat_at':NULL,
                'last_chat_id':NULL,
                'created_at':created_at
            }
            writer.writerow(row)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name}]

# chatroom_id별 member_chatroom csv 생성
def generate_member_chatroom_csv(sampled_member_pairs,created_chatrooms):
    member_chatroom_rows = []
    for (member_id1, member_id2), (chatroom_id, created_at) in zip(sampled_member_pairs, created_chatrooms):
        last_join_date = rm.generate_sorted_after_created_at(str(created_at), 1)[0]  # chatroom의 created_at 이후 ~ 오늘까지 중 랜덤 추출
        member_chatroom_rows.append({
            "chatroom_id": chatroom_id,
            "member_id": member_id1,
            "last_join_date": last_join_date,
            "last_view_date": NULL,
            'created_at':created_at
        })
        member_chatroom_rows.append({
            "chatroom_id": chatroom_id,
            "member_id": member_id2,
            "last_join_date": last_join_date,
            "last_view_date": NULL,
            'created_at':created_at
        })
    
    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{MEMBER_CHATROOM_TABLE}_{len(created_chatrooms)*2}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=MEMBER_CHATROOM_HEADERS)
        writer.writeheader()
        for row in member_chatroom_rows:
            writer.writerow(row)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name, }]

def run(**kwargs):
    k = kwargs.get("rooms") # 생성할 채팅방 개수

    # step 1: member id 조회
    print(f"[1] Fetching member ids...")
    member_ids = [row[0] for row in db_fetcher.fetch_columns("member", ["member_id"])]

    # step 2: 기존 member_chatroom에서 참여 쌍 조회 및 샘플링
    print(f"[2] Fetching member_chatrooms and sampling pairs...")
    raw = db_fetcher.fetch_columns(MEMBER_CHATROOM_TABLE, ["chatroom_id", "member_id"])
    chatroom_to_members = {}
    for room_id, member_id in raw:
        chatroom_to_members.setdefault(room_id, []).append(member_id)

    # 기존 참여 쌍 추출
    existing_pairs = set()
    for member_list in chatroom_to_members.values():
        if len(member_list) == 2:
            a, b = sorted(member_list)
            existing_pairs.add((a, b))

    # 생성 가능한 최대 쌍 계산
    all_possible_pairs = list(combinations(member_ids, 2))  # (1,2)==(2,1)
    available_pairs = [pair for pair in all_possible_pairs if pair not in existing_pairs]

    if k > len(available_pairs):
        raise ValueError(f"❌ 생성 가능 최대 쌍({len(available_pairs)})보다 큰 입력({k})입니다.")

    sampled_pairs = random.sample(available_pairs, k) # k개의 (member_id1,member_id2) 쌍 추출

    # step 3: chatroom csv 생성
    print(f"[3] Generating {k} chatroom rows...")
    start = time.time()
    try:
        generated = generate_chatroom_csv(k)
    except ValueError as ve:
        print(f"Error occuerd while generate_chatroom_csv, {ve}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")

    chatroom_filepath = generated[0]["filepath"]
    chatroom_filename = generated[0]["filename"]

    # step 4: chatroom RDS 업로드
    print(f"[4] LOAD DATA LOCAL INFILE - {chatroom_filename} to RDS...")
    start = time.time()
    uploader.load_csv_with_local_infile(chatroom_filepath, CHATROOM_TABLE)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 5: chatroom id 및 created_at 필드 조회
    print(f"[5] Fetching chatroom...")
    created_chatrooms = db_fetcher.fetch_columns(CHATROOM_TABLE, ["chatroom_id", "created_at"])
    created_chatrooms = created_chatrooms[-k:]  # 방금 삽입된 k개

    # step 6: member_chatroom csv 생성
    print(f"[6] Generating {k} * 2 member_chatroom rows...")
    start = time.time()
    try:
        generated = generate_member_chatroom_csv(sampled_pairs,created_chatrooms)
    except Exception as error:
        print(f"Error occuerd while generate_member_chatroom_csv, {error}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")
    
    member_chatroom_filepath = generated[0]["filepath"]
    member_chatroom_filename = generated[0]["filename"]

    # step 7: member_chatroom RDS 업로드
    print(f"[7] LOAD DATA LOCAL INFILE - {member_chatroom_filename} to RDS...")
    start = time.time()
    uploader.load_csv_with_local_infile(member_chatroom_filepath, MEMBER_CHATROOM_TABLE)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 8, 9: S3 업로드
    print(f"[8] Uploading {chatroom_filename} to S3...")
    start = time.time()
    uploader.upload_to_s3(chatroom_filepath, chatroom_filename)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

    print(f"[9] Uploading {member_chatroom_filename} to S3...")
    start = time.time()
    uploader.upload_to_s3(member_chatroom_filepath, member_chatroom_filename)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

