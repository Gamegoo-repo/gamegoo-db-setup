import csv
import time
from datetime import datetime
from modules import uploader
from modules import db_fetcher
from modules import random_modules as rm

TABLE_NAME = 'member_recent_stats'
HEADERS = ['member_id','rec_avg_assists','rec_avg_cs_per_minute','rec_avg_deaths','rec_avgkda',
           'rec_total_losses','rec_total_wins','rec_avg_kills','rec_total_cs','rec_win_rate']

# csv 생성
def generate_member_recent_stats_csv(member_ids):
    rows = len(member_ids)

    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{TABLE_NAME}_{rows}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        for member_id in member_ids:
            row={ 
                'member_id':member_id,
                'rec_avg_assists':rm.sample_integer(0,600),
                'rec_avg_cs_per_minute':rm.sample_float(0,15),
                'rec_avg_deaths':rm.sample_integer(0,600),
                'rec_avgkda':rm.sample_integer(0,30),
                'rec_win_rate':rm.sample_float(0,100),
                'rec_avg_kills':rm.sample_integer(0,600),
                'rec_total_losses':rm.sample_integer(0,30),
                'rec_total_wins':rm.sample_integer(0,30),
                'rec_total_cs':rm.sample_integer(0,8000),
            }
            writer.writerow(row)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name}]


def run(**kwargs):
    # step 1: member id 조회
    print(f"[1] Fetching member ids...")
    member_ids = [row[0] for row in db_fetcher.fetch_columns("member", ["member_id"])]

    # step 2: member_recent_stats 테이블 데이터 초기화 
    print(f"[2] DELETE all rows...")
    db_fetcher.delete_all_rows(TABLE_NAME)

    # step 3: csv 생성
    print(f"[3] Generating {len(member_ids)} member_recent_stats rows...")
    start = time.time()
    try:
        generated = generate_member_recent_stats_csv(member_ids)
    except ValueError as ve:
        print(f"Error occuerd while generate_member_recent_stats_csv, {ve}")
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
