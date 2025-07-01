import csv
import time
import random
from itertools import combinations
from datetime import datetime
from modules import uploader
from modules import db_fetcher
from modules import random_modules as rm

TABLE_NAME = 'member_game_style'
HEADERS = ['member_id', 'gamestyle_id','created_at']


# 조합 생성 + csv 생성
def generate_member_game_style_csv(member_ids, k):
    rows = len(member_ids)
    sorted_dates = rm.generate_sorted_created_at_list(rows,30)

    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{TABLE_NAME}_{rows*k}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        for member_id, created_at in zip(member_ids, sorted_dates):
            sampled_game_style_ids = rm.sample_integers(1,17,k)
            for game_style_id in sampled_game_style_ids:
                row={ 
                    'member_id':member_id,
                    'gamestyle_id':game_style_id,
                    'created_at':created_at
                }
                writer.writerow(row)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name}]


def run(**kwargs):
    k = kwargs.get("per_member")

    # step 1: member id 조회
    print(f"[1] Fetching member ids...")
    member_ids = [row[0] for row in db_fetcher.fetch_columns("member", ["member_id"])]

    # step 2: member_game_style 테이블 데이터 초기화 
    print(f"[2] DELETE all rows...")
    db_fetcher.delete_all_rows(TABLE_NAME)

    # step 3: csv 생성
    print(f"[3] Generating {len(member_ids)} * {k} member_game_style pairs...")
    start = time.time()
    try:
        generated = generate_member_game_style_csv(member_ids, k)
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
