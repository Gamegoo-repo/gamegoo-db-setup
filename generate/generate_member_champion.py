import csv
import time
import random
from datetime import datetime
from modules import uploader
from modules import db_fetcher
from modules import random_modules as rm

TABLE_NAME = 'member_champion'
HEADERS = ['member_id', 'champion_id','assists','cs_per_minute','deaths',
           'games','kills','wins','total_cs','created_at']


# 조합 생성 + csv 생성
def generate_member_champion_csv(member_ids, champion_ids,k):
    rows = len(member_ids)
    sorted_dates = rm.generate_sorted_created_at_list(rows,30)

    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{TABLE_NAME}_{rows*k}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        for member_id, created_at in zip(member_ids, sorted_dates):
            sampled_champion_ids = random.sample(champion_ids,k)
            for champion_id in sampled_champion_ids:
                row={ 
                    'member_id':member_id,
                    'champion_id':champion_id,
                    'assists':rm.sample_integer(0,600),
                    'cs_per_minute':rm.sample_float(0,15),
                    'deaths':rm.sample_integer(0,600),
                    'games':rm.sample_integer(0,30),
                    'kills':rm.sample_integer(0,600),
                    'wins':rm.sample_integer(0,30),
                    'total_cs':rm.sample_integer(0,8000),
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

    # step 2: champion id 조회
    print(f"[2] Fetching champion ids...")
    champion_ids = [row[0] for row in db_fetcher.fetch_columns("champion", ["champion_id"])]

    # step 3: member_champion 테이블 데이터 초기화 
    print(f"[3] DELETE all rows...")
    db_fetcher.delete_all_rows(TABLE_NAME)

    # step 3: csv 생성
    print(f"[3] Generating {len(member_ids)} * {k} member_champion rows...")
    start = time.time()
    try:
        generated = generate_member_champion_csv(member_ids,champion_ids, k)
    except ValueError as ve:
        print(f"Error occuerd while generate_member_champion_csv, {ve}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")

    filepath = generated[0]["filepath"]
    filename = generated[0]["filename"]

    # step 4: S3 업로드
    print(f"[4] Uploading to S3...")
    start = time.time()
    uploader.upload_to_s3(filepath, filename)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 5: RDS 업로드
    print(f"[5] LOAD DATA LOCAL INFILE to RDS...")
    start = time.time()
    uploader.load_csv_with_local_infile(filepath, TABLE_NAME)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")
