import csv
import time
import random
from itertools import permutations
from datetime import datetime
from modules import uploader
from modules import db_fetcher
from modules import enums
from modules import random_modules as rm
from datetime import datetime, timedelta

MANNER_RATING_TABLE = 'manner_rating'
MANNER_RATING_HEADERS = ['from_member_id', 'to_member_id','positive','created_at']

RATING_KEYWORD_TABLE = 'manner_rating_keyword'
RATING_KEYWORD_HEADERS=['manner_rating_id','manner_keyword_id','created_at']

NULL = '\\N'

# manner_rating 조합 생성 + csv 생성
def generate_manner_rating_csv(member_ids, rows):
    all_pairs = list(permutations(member_ids, 2))  # (A, B) → A ≠ B, (1,2) ≠ (2,1)
    if rows > len(all_pairs):
        raise ValueError(f"가능한 조합수({len(all_pairs)})보다 큰 row값은 부적합합니다.")

    sorted_dates = rm.generate_sorted_created_at_list(rows,30) #  created_at 추출
    sampled = random.sample(all_pairs, rows)

    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{MANNER_RATING_TABLE}_{rows}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=MANNER_RATING_HEADERS)
        writer.writeheader()
        for (from_member_id,to_member_id), created_at in zip(sampled,sorted_dates):
            row={ 
                'from_member_id': from_member_id,
                'to_member_id':to_member_id,
                'positive':rm.sample_integer(0,1),
                'created_at':created_at
            }
            writer.writerow(row)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name, }]

# positive 여부에 따른 manner_keyword_id 추출
def get_manner_keyword_id(positive,k):
    if positive:
        return rm.sample_integers(1,6,k)
    else:
        return rm.sample_integers(7,12,k)

# manner_rating_id별 manner_rating_keyword csv 생성
def generate_manner_rating_keyword_csv(manner_ratings):
    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{RATING_KEYWORD_TABLE}_{len(manner_ratings)*3}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    print(manner_ratings)

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=RATING_KEYWORD_HEADERS)
        writer.writeheader()
        for (manner_rating_id,positive,created_at) in manner_ratings:
            manner_keyword_ids = get_manner_keyword_id(positive,3)
            for manner_keyword_id in manner_keyword_ids: # 한 manner_rating_id 당 3개의 row
                row={ 
                    'manner_rating_id': manner_rating_id,
                    'manner_keyword_id':manner_keyword_id,
                    'created_at':created_at
                }
                writer.writerow(row)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name, }]


def run(**kwargs):
    rows = kwargs.get("rows")

    # step 1: member id 조회
    print(f"[1] Fetching member ids...")
    member_ids = [row[0] for row in db_fetcher.fetch_columns("member", ["member_id"])]

    # step 2: manner_rating_keyword 테이블 데이터 초기화 
    print(f"[2] DELETE all rows from {RATING_KEYWORD_TABLE}...")
    db_fetcher.delete_all_rows(RATING_KEYWORD_TABLE)

    # step 3: manner_rating 테이블 데이터 초기화 
    print(f"[3] DELETE all rows from {MANNER_RATING_TABLE}...")
    db_fetcher.delete_all_rows(MANNER_RATING_TABLE)

    # step 4: manner_rating csv 생성
    print(f"[4] Generating {rows} manner_rating rows...")
    start = time.time()
    try:
        generated = generate_manner_rating_csv(member_ids,rows)
    except ValueError as ve:
        print(f"Error occuerd while generate_manner_rating_csv, {ve}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")

    manner_rating_filepath = generated[0]["filepath"]
    manner_rating_filename = generated[0]["filename"]

    # step 5: manner_rating RDS 업로드
    print(f"[5] LOAD DATA LOCAL INFILE - {manner_rating_filename} to RDS...")
    start = time.time()
    uploader.load_csv_with_local_infile(manner_rating_filepath, MANNER_RATING_TABLE)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 6: manner_rating id 및 positive 필드 조회
    print(f"[6] Fetching manner_rating...")
    manner_ratings = [[row[0],row[1],row[2]] for row in db_fetcher.fetch_columns(MANNER_RATING_TABLE, ["manner_rating_id","positive","created_at"])]

    # step 7: manner_rating_keyword csv 생성
    print(f"[7] Generating {rows} * 3 manner_rating_keyword rows...")
    start = time.time()
    try:
        generated = generate_manner_rating_keyword_csv(manner_ratings)
    except Exception as error:
        print(f"Error occuerd while generate_manner_rating_keyword_csv, {error}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")
    
    rating_keyword_filepath = generated[0]["filepath"]
    rating_keyword_filename = generated[0]["filename"]

    # step 8: manner_rating_keyword RDS 업로드
    print(f"[8] LOAD DATA LOCAL INFILE - {rating_keyword_filename} to RDS...")
    start = time.time()
    uploader.load_csv_with_local_infile(rating_keyword_filepath, RATING_KEYWORD_TABLE)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 9, 10: S3 업로드
    print(f"[9] Uploading {manner_rating_filename} to S3...")
    start = time.time()
    uploader.upload_to_s3(manner_rating_filepath, manner_rating_filename)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

    print(f"[10] Uploading {rating_keyword_filename} to S3...")
    start = time.time()
    uploader.upload_to_s3(rating_keyword_filepath, rating_keyword_filename)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

