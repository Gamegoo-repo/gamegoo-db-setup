import csv
import time
import random
import uuid
from datetime import datetime
from modules import uploader
from modules import db_fetcher
from modules import random_modules as rm
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

CHATROOM_TABLE = 'chatroom'
MEMBER_CHATROOM_TABLE = 'member_chatroom'

CHAT_TABLE = "chat"
CHAT_HEADERS=['chatroom_id','from_member_id','to_member_id','source_board_id','system_type',
              'contents','timestamp','created_at']

NULL = '\\N'

# chat csv 생성
def generate_chat_csv(room_map,sampled_room_ids,rows_per_room):
    # 각 채팅방에 대해 rows개의 chat row 생성
    chat_rows = []
    for room_id in sampled_room_ids:
        info = room_map[room_id]
        member_ids = info["member_ids"]
        base_dt = info["last_chat_at"] or info["chatroom_created_at"]  # NULL 처리

        dt_list = rm.generate_sorted_after_created_at(
            base_dt.strftime("%Y-%m-%d %H:%M:%S.%f"),
            rows_per_room
        ) # chat의 created_at 리스트 추출

        for i in range(rows_per_room):
            dt = dt_list[i]
            timestamp = int(datetime.strptime(dt, "%Y-%m-%d %H:%M:%S.%f").timestamp() * 1000)
            chat_rows.append({
                "chatroom_id": room_id,
                "from_member_id": random.choice(member_ids),
                "to_member_id":NULL,
                "source_board_id":NULL,
                "system_type":NULL,
                "contents": fake.sentence(),
                "created_at": dt,
                "timestamp": timestamp
            })

    # 전체 chat row를 created_at 기준으로 정렬
    chat_rows.sort(key=lambda row: row["created_at"])

    # chat.csv 생성
    file_name = f"{CHAT_TABLE}_{len(chat_rows)}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"
    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CHAT_HEADERS)
        writer.writeheader()
        for row in chat_rows:
            writer.writerow(row)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name, }]

def run(**kwargs):
    chatroom_count = kwargs.get("rooms") # 메시지를 생성할 채팅방 개수
    rows_per_room = kwargs.get("rows") # 한 채팅방 당 생성할 메시지 개수

    # step 1: member_chatroom에서 member_id, chatroom_id 조회
    print(f"[1] Fetching member_chatroom...")
    mc_rows = db_fetcher.fetch_columns(MEMBER_CHATROOM_TABLE, ["chatroom_id", "member_id"])

    # step 2: chatroom에서 chatroom_id, last_chat_at, created_at 조회
    print(f"[2] FFetching chatroom...")
    cr_rows = db_fetcher.fetch_columns(CHATROOM_TABLE, ["chatroom_id", "last_chat_at", "created_at"])

    # step 3: dict 구성 및 채팅방 샘플링
    print(f"[3] Extracting info as dict and sampling chatrooms...")
    room_map = {}
    for chatroom_id, member_id in mc_rows:
        room_map.setdefault(chatroom_id, {
            "member_ids": [],
            "chatroom_created_at": None,
            "last_chat_at": None
        })
        room_map[chatroom_id]["member_ids"].append(member_id)

    for chatroom_id, last_chat_at, cr_created in cr_rows:
        if chatroom_id in room_map:
            room_map[chatroom_id]["chatroom_created_at"] = cr_created
            room_map[chatroom_id]["last_chat_at"] = last_chat_at
    
    # chatroom_id n개 랜덤 추출
    available_room_ids = [rid for rid, v in room_map.items() if len(v["member_ids"]) == 2]
    if chatroom_count > len(available_room_ids):
        raise ValueError("요청한 채팅방 개수보다 실제 존재하는 채팅방 수가 적습니다.")
    sampled_room_ids = random.sample(available_room_ids, chatroom_count)

    # step 4: chatroom csv 생성
    print(f"[4] Generating {chatroom_count} * {rows_per_room} chat rows...")
    start = time.time()
    try:
        generated = generate_chat_csv(room_map,sampled_room_ids,rows_per_room)
    except ValueError as ve:
        print(f"Error occuerd while generate_chat_csv, {ve}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")

    chat_filepath = generated[0]["filepath"]
    chat_filename = generated[0]["filename"]

    # step 5: chat RDS 업로드
    print(f"[5] LOAD DATA LOCAL INFILE - {chat_filename} to RDS...")
    start = time.time()
    uploader.load_csv_with_local_infile(chat_filepath, CHAT_TABLE)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 6: chat 테이블에서 각 채팅방의 마지막 메시지 추출
    print(f"[6] Extracting latest chat from chatroom...")
    start = time.time()
    try:
        room_id_str = ','.join(map(str, sampled_room_ids))
        latest_rows = db_fetcher.fetch_query(f"""
            SELECT c1.chatroom_id, c1.created_at, c1.chat_id
            FROM chat c1
            JOIN (
                SELECT chatroom_id, MAX(created_at) AS max_created
                FROM chat
                WHERE chatroom_id IN ({room_id_str})
                GROUP BY chatroom_id
            ) c2 ON c1.chatroom_id = c2.chatroom_id AND c1.created_at = c2.max_created
        """)  # [(chatroom_id, created_at, chat_id)]
    except Exception as error:
        print(f"Error occuerd while Extracting latest chat from chatroom, {error}")
        return
    print(f"⏱️ 채팅방 마지막 메시지 추출 소요 시간: {time.time() - start:.2f}초\n")

    # step 7: chatroom 테이블 업데이트
    print(f"[7] Updating {len(latest_rows)} rows from chatroom...")
    start = time.time()
    try:
        conn, cursor = db_fetcher.get_connection_and_cursor()

        sql = """
            UPDATE chatroom
            SET last_chat_at = %s, last_chat_id = %s, updated_at = %s
            WHERE chatroom_id = %s
        """

        batch_size = 1000
        total_updated = 0

        for i in range(0, len(latest_rows), batch_size):
            batch = latest_rows[i:i + batch_size]
            values = [(created_at, chat_id, created_at, chatroom_id) for chatroom_id, created_at, chat_id in batch]
            cursor.executemany(sql, values)
            total_updated += len(values)
            print("batch updated:",i)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as error:
        print(f"Error occurred while updating chatroom: {error}")
        return
    print(f"✅ chatroom 테이블 배치 업데이트 완료: {total_updated} rows")
    print(f"⏱️ chatroom 테이블 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 8: S3 업로드
    print(f"[8] Uploading {chat_filename} to S3...")
    start = time.time()
    uploader.upload_to_s3(chat_filepath, chat_filename)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

