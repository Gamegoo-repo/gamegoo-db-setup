import csv
import time
import random
import uuid
from datetime import datetime
from modules import uploader
from modules import db_fetcher
from modules import enums
from modules import random_modules as rm
from datetime import datetime, timedelta

MATCHING_RECORD_TABLE = 'matching_record'
MATCHING_RECORD_HEADERS = ['matching_uuid', 'member_id','target_matching_uuid',
           'game_mode','matching_type','game_rank','manner_level','winrate',
           'mainp','subp','tier','mike',
           'status','manner_message_sent','created_at']

WANT_POSITIONS_TABLE = 'matching_record_want_positions'
WANT_POSITIONS_HEADERS=['matching_uuid','want_position']

NULL = '\\N'
MANNER_MESSAGE_SENT="NOT_REQUIRED"

# matching_status 결정
def get_status_by_time(dt_str: str) -> str:
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    now = datetime.now()

    # 시간 차이 계산
    delta = now - dt

    return "PENDING" if timedelta(minutes=0) <= delta <= timedelta(minutes=4) else "QUIT"    

# matching_record 조합 생성 + csv 생성
def generate_matching_record_csv(member_ids, k,day):
    sorted_dates = rm.generate_sorted_created_at_list(len(member_ids),day) #  created_at 추출
    shuffled_member_ids = random.sample(member_ids, len(member_ids)) # member_id 섞기

    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{MATCHING_RECORD_TABLE}_{len(member_ids)*k}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    matching_record_uuids=[]

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=MATCHING_RECORD_HEADERS)
        writer.writeheader()
        for member_id, base_created_at in zip(shuffled_member_ids,sorted_dates):
            created_at_list=rm.generate_slots_from_base(base_created_at,k,6)
            for i in range(k): # 한 회원 당 k개의 row
                matching_uuid = uuid.uuid4()
                row={ 
                    'matching_uuid': matching_uuid,
                    'member_id':member_id,
                    'target_matching_uuid':NULL,
                    'game_mode':random.choice(enums.GAMEMODE),
                    'matching_type':random.choice(enums.MATCHING_TYPE),
                    'game_rank':rm.sample_integer(1,4),
                    'manner_level':rm.sample_integer(1,5),
                    'winrate':rm.sample_float(0, 100),
                    'mainp':random.choice(enums.POSITION),
                    'subp':random.choice(enums.POSITION),
                    'tier':random.choice(enums.TIER),
                    'mike':random.choice(enums.MIKE),
                    'status':get_status_by_time(created_at_list[i]),
                    'manner_message_sent':MANNER_MESSAGE_SENT,
                    'created_at':created_at_list[i]
                }
                writer.writerow(row)
                matching_record_uuids.append(matching_uuid)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name, },matching_record_uuids]

# matching_uuid별 want_position csv 생성
def generate_matching_record_want_positions_csv(matching_record_uuids):
    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{WANT_POSITIONS_TABLE}_{len(matching_record_uuids)*2}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=WANT_POSITIONS_HEADERS)
        writer.writeheader()
        for matching_uuid in matching_record_uuids:
            sampled_want_position = random.sample(enums.POSITION,2)
            for want_position in sampled_want_position: # 한 uuid 당 2개의 row
                row={ 
                    'matching_uuid': matching_uuid,
                    'want_position':want_position
                }
                writer.writerow(row)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name, }]


def run(**kwargs):
    k = kwargs.get("per_member")
    day_range = kwargs.get("day_limit")

    # step 1: member id 조회
    print(f"[1] Fetching member ids...")
    member_ids = [row[0] for row in db_fetcher.fetch_columns("member", ["member_id"])]

    # step 2: matching_record_want_positions 테이블 데이터 초기화 
    print(f"[2] DELETE all rows from {WANT_POSITIONS_TABLE}...")
    db_fetcher.delete_all_rows(WANT_POSITIONS_TABLE)

    # step 3: matching_record 테이블 데이터 초기화 
    print(f"[3] DELETE all rows from {MATCHING_RECORD_TABLE}...")
    db_fetcher.delete_all_rows(MATCHING_RECORD_TABLE)

    # step 4: matching_record csv 생성
    print(f"[4] Generating {len(member_ids)} * {k} matching_record rows...")
    start = time.time()
    try:
        generated = generate_matching_record_csv(member_ids, k,day_range)
    except ValueError as ve:
        print(f"Error occuerd while generate_matching_record_csv, {ve}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")

    matching_record_filepath = generated[0]["filepath"]
    matching_record_filename = generated[0]["filename"]
    generated_uuids=generated[1] 

    # step 5: matching_record_want_position csv 생성
    print(f"[5] Generating {len(generated_uuids)} matching_record_want_position rows...")
    start = time.time()
    try:
        generated = generate_matching_record_want_positions_csv(generated_uuids)
    except Exception as error:
        print(f"Error occuerd while generate_matching_record_want_positions_csv, {error}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")
    
    want_position_filepath = generated[0]["filepath"]
    want_position_filename = generated[0]["filename"]

    # step 6, 7: S3 업로드
    print(f"[6] Uploading {matching_record_filename} to S3...")
    start = time.time()
    uploader.upload_to_s3(matching_record_filepath, matching_record_filename)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

    print(f"[7] Uploading {want_position_filename} to S3...")
    start = time.time()
    uploader.upload_to_s3(want_position_filepath, want_position_filename)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 8, 9: RDS 업로드
    print(f"[8] LOAD DATA LOCAL INFILE - {matching_record_filename} to RDS...")
    start = time.time()
    uploader.load_csv_with_local_infile(matching_record_filepath, MATCHING_RECORD_TABLE)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

    print(f"[9] INSERT ALL ROWS - {want_position_filename} to RDS...")
    start = time.time()
    uploader.insert_rows_from_csv_batch(want_position_filepath, WANT_POSITIONS_TABLE)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")