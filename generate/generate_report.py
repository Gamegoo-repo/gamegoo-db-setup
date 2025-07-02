import csv
import time
import random
from datetime import datetime
from modules import uploader
from modules import db_fetcher
from modules import enums
from modules import random_modules as rm
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

REPORT_TABLE = 'report'
REPORT_HEADERS = ['from_member_id', 'to_member_id','content','board_id','path','created_at']

REPORT_TYPE_TABLE = 'report_type_mapping'
REPORT_TYPE_HEADERS=['report_id','code','created_at']

NULL = '\\N'
PATH_BOARD="BOARD"
PATH_CHAT="CHAT"
PATH_PROFILE="PROFILE"

# report csv 생성
def generate_report_csv(member_ids,boards,rows):
    sorted_dates = rm.generate_sorted_created_at_list(rows,30) #  created_at 추출

    # BOARD path용 report 생성
    all_reports = []
    board_ids = [r[0] for r in boards]
    board_id_to_owner = {r[0]: r[1] for r in boards}

    for i in range(rows):
        b_id = random.choice(board_ids)
        to_id = board_id_to_owner[b_id]
        from_id = random.choice(member_ids)
        if from_id == to_id:
            continue
        all_reports.append({
            "from_member_id": from_id,
            "to_member_id": to_id,
            "path": PATH_BOARD,
            "board_id": b_id,
            "content": fake.paragraphs(nb=1)[0],
            "created_at": sorted_dates[i]
        })
        if len(all_reports) >= rows:
            break

    # CHAT / PROFILE report 생성
    rest = rows - len(all_reports)
    alt_paths = [PATH_CHAT,PATH_PROFILE]
    alt_reports = []
    i = len(all_reports)

    while len(alt_reports) < rest:
        from_id, to_id = random.sample(member_ids, 2)
        alt_reports.append({
            "from_member_id": from_id,
            "to_member_id": to_id,
            "path": random.choice(alt_paths),
            "board_id": NULL,
            "content": fake.paragraphs(nb=1)[0],
            "created_at": sorted_dates[i]
        })
        i+=1

    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{REPORT_TABLE}_{rows}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_HEADERS)
        writer.writeheader()
        for row in all_reports:
            writer.writerow(row)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name, }]

# report_id별 report_type_mapping csv 생성
def generate_report_type_mapping_csv(reports):
    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{REPORT_TYPE_TABLE}_{len(reports)*2}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_TYPE_HEADERS)
        writer.writeheader()
        for (report_id,created_at) in reports:
            sampled_code = rm.sample_integers(1,6,2)
            for code in sampled_code: # 한 report_id 당 2개의 row
                row={ 
                    'report_id': report_id,
                    'code':code,
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

    # step 2: board id 및 member_id 필드 조회
    print(f"[2] Fetching board...")
    boards = db_fetcher.fetch_columns("board", ["board_id", "member_id"])

    # step 3: report_type_mapping 테이블 데이터 초기화 
    print(f"[3] DELETE all rows from {REPORT_TYPE_TABLE}...")
    db_fetcher.delete_all_rows(REPORT_TYPE_TABLE)

    # step 4: report 테이블 데이터 초기화 
    print(f"[4] DELETE all rows from {REPORT_TABLE}...")
    db_fetcher.delete_all_rows(REPORT_TABLE)

    # step 5: report csv 생성
    print(f"[5] Generating {rows} report rows...")
    start = time.time()
    try:
        generated = generate_report_csv(member_ids,boards,rows)
    except ValueError as ve:
        print(f"Error occuerd while generate_report_csv, {ve}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")

    report_filepath = generated[0]["filepath"]
    report_filename = generated[0]["filename"]

    # step 6: board RDS 업로드
    print(f"[6] LOAD DATA LOCAL INFILE - {report_filename} to RDS...")
    start = time.time()
    uploader.load_csv_with_local_infile(report_filepath, REPORT_TABLE)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 7: report id 및 created_at 필드 조회
    print(f"[7] Fetching report...")
    reports = db_fetcher.fetch_columns(REPORT_TABLE, ["report_id", "created_at"])

    # step 8: report_type_mapping csv 생성
    print(f"[8] Generating {rows} * 2 report_type_mapping rows...")
    start = time.time()
    try:
        generated = generate_report_type_mapping_csv(reports)
    except Exception as error:
        print(f"Error occuerd while generate_report_type_mapping_csv, {error}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")
    
    type_mapping_filepath = generated[0]["filepath"]
    type_mapping_filename = generated[0]["filename"]

    # step 9: report_type_mapping RDS 업로드
    print(f"[9] LOAD DATA LOCAL INFILE - {type_mapping_filename} to RDS...")
    start = time.time()
    uploader.load_csv_with_local_infile(type_mapping_filepath, REPORT_TYPE_TABLE)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 10, 11: S3 업로드
    print(f"[12] Uploading {report_filename} to S3...")
    start = time.time()
    uploader.upload_to_s3(report_filepath, report_filename)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

    print(f"[13] Uploading {type_mapping_filename} to S3...")
    start = time.time()
    uploader.upload_to_s3(type_mapping_filepath, type_mapping_filename)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

