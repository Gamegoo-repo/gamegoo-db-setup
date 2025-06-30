import csv
import time
import random
from itertools import permutations
from datetime import datetime
from modules import uploader
from modules import db_fetcher
from modules import enums
from modules import random_modules as rm

TABLE_NAME = 'notification'
HEADERS = ['notification_type_id', 'member_id','source_member_id','content','is_read','created_at']

# 조합 생성 + csv 생성
def generate_notification_csv(member_ids, rows):
    all_pairs = list(permutations(member_ids, 2))  # (A, B) → A ≠ B, (1,2) ≠ (2,1)

    if rows > len(all_pairs):
        raise ValueError(f"가능한 조합수({len(all_pairs)})보다 큰 row값은 부적합합니다.")

    sampled = random.sample(all_pairs, rows)
    sorted_dates = rm.generate_sorted_created_at_list(rows,30)

    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{TABLE_NAME}_{rows}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        for (member_id, source_member_id),created_at in zip(sampled,sorted_dates):
            notification_type_id = rm.sample_integer(1,4)
            row={
                'notification_type_id':notification_type_id,
                'member_id':member_id,
                'source_member_id':source_member_id,
                'content':enums.NOTIFICATION_CONTENTS[notification_type_id],
                'is_read':rm.sample_integer(0,1),
                'created_at':created_at
            }
            writer.writerow(row)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name}]


def run(**kwargs):
    rows = kwargs.get("rows")

    # step 1: member id 조회
    print(f"[1] Fetching member ids...")
    member_ids = [row[0] for row in db_fetcher.fetch_columns("member", ["member_id"])]

    # step 2: notification 테이블 데이터 초기화 
    print(f"[2] DELETE all rows...")
    db_fetcher.delete_all_rows(TABLE_NAME)

    # step 3: csv 생성
    print(f"[3] Generating {rows} notification rows...")
    start = time.time()
    try:
        generated = generate_notification_csv(member_ids, rows)
    except ValueError as ve:
        print(f"Error occuerd while generate_notification_csv, {ve}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")

    filepath = generated[0]["filepath"]
    filename = generated[0]["filename"]

    # step 4: S3 업로드
    # print(f"[4] Uploading to S3...")
    # start = time.time()
    # uploader.upload_to_s3(filepath, filename)
    # print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 5: RDS 업로드
    print(f"[5] LOAD DATA LOCAL INFILE to RDS...")
    start = time.time()
    uploader.load_csv_with_local_infile(filepath, TABLE_NAME)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")
