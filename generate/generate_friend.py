import csv
import time
import random
from itertools import combinations
from datetime import datetime
from modules import uploader
from modules import db_fetcher
from modules import random_modules as rm

TABLE_NAME = 'friend'
HEADERS = ['from_member_id', 'to_member_id','liked','created_at']


# 조합 생성 + csv 생성
def generate_friend_csv(member_ids, pairs):
    all_pairs = list(combinations(member_ids, 2))  # (A, B) → A ≠ B, (1,2) == (2,1)

    if pairs > len(all_pairs):
        raise ValueError(f"가능한 조합수({len(all_pairs)})보다 큰 row값은 부적합합니다.")

    sampled = random.sample(all_pairs, pairs)
    sorted_dates = rm.generate_sorted_created_at_list(pairs,30)

    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{TABLE_NAME}_{pairs}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        for (from_member_id, to_member_id), created_at in zip(sampled, sorted_dates):
            row={ 
                'from_member_id':from_member_id,
                'to_member_id':to_member_id,
                'liked':rm.sample_integer(0,1),
                'created_at':created_at
            }
            writer.writerow(row) # A -> B row 삽입

            row = {
                'from_member_id':to_member_id,
                'to_member_id':from_member_id,
                'liked':rm.sample_integer(0,1),
                'created_at':created_at
            }
            writer.writerow(row) # B -> A row 삽입

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name}]


def run(**kwargs):
    pairs = kwargs.get("pairs")

    # step 1: member id 조회
    print(f"[1] Fetching member ids...")
    member_ids = [row[0] for row in db_fetcher.fetch_columns("member", ["member_id"])]

    # step 2: friend 테이블 데이터 초기화 
    print(f"[2] DELETE all rows...")
    db_fetcher.delete_all_rows(TABLE_NAME)

    # step 3: csv 생성
    print(f"[3] Generating {pairs} friend pairs...")
    start = time.time()
    try:
        generated = generate_friend_csv(member_ids, pairs)
    except ValueError as ve:
        print(f"Error occuerd while generate_friend_csv, {ve}")
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
