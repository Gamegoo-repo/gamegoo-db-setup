import csv
import random
import uuid
import time
from faker import Faker
from modules import enums
from datetime import datetime
from modules import uploader as uploader
from modules import random_modules as rm


TABLE_NAME='member'
PASSWORD = '$2a$10$OfT6f2rP7qHDLSk/2LXlh.QM6EnM0.ZWIf/nZwpufJ0YBQtvRkwlC'
MANNER_LEVEL = 1
BLIND = 0
LOGIN_TYPE = 'GENERAL'
TAG = 'KR1'
IS_AGREE = 1

HEADERS = [
    'email', 'puuid', 'password', 'profile_image', 'manner_level', 'blind',
    'login_type', 'game_name', 'tag', 'solo_tier', 'solo_rank', 'solo_win_rate',
    'free_tier', 'free_rank', 'free_win_rate', 'mainp', 'subp', 'mike',
    'solo_game_count', 'free_game_count', 'is_agree',
    'created_at'
]

fake = Faker()

def generate_unique_email():
    base_email = fake.email()  # 예: user123@gmail.com
    username, domain = base_email.split('@')

    # uuid4의 앞 8자리만 사용 (16진수 4바이트 → 42억 개 조합)
    uid = uuid.uuid4().hex[:8]  # 예: a3f9c7b2

    return f"{username}_{uid}@{domain}"

def generate_csv(rows):
    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    FILE_NAME = f"{TABLE_NAME}_{rows}r_{timestamp}.csv"
    FILE_PATH = f"./csv/{FILE_NAME}"
    with open(FILE_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()

        for _ in range(rows):
            row = {
                'email': generate_unique_email(),
                'puuid': rm.generate_random_string(78),
                'password': PASSWORD,
                'profile_image': rm.sample_integer(1, 8),
                'manner_level': MANNER_LEVEL,
                'blind': BLIND,
                'login_type': LOGIN_TYPE,
                'game_name': fake.user_name(),
                'tag': TAG,
                'solo_tier': random.choice(enums.TIER),
                'solo_rank': rm.sample_integer(1, 4),
                'solo_win_rate': rm.sample_float(0, 100),
                'free_tier': random.choice(enums.TIER),
                'free_rank': rm.sample_integer(1, 4),
                'free_win_rate': rm.sample_float(0, 100),
                'mainp': random.choice(enums.POSITION),
                'subp': random.choice(enums.POSITION),
                'mike': random.choice(enums.MIKE),
                'solo_game_count': rm.sample_integer(0, 1000),
                'free_game_count': rm.sample_integer(0, 1000),
                'is_agree': IS_AGREE,
                'created_at': rm.sample_created_at(100)
            }
            writer.writerow(row)
    
    print(f"csv created at {FILE_PATH}")
    return [{"filepath":FILE_PATH, "filename":FILE_NAME}]

def run(**kwargs):
    # step 1: csv 생성
    print(f"[1] Generate csv ...")
    rows = kwargs.get("rows", 10)
    start = time.time()
    generated = generate_csv(rows)
    print(f"⏱️ csv 생성 소요 시간: {time.time() - start:.2f}초\n")

    # step 2: S3 업로드
    start = time.time()
    filepath = generated[0]["filepath"]
    filename = generated[0]["filename"]
    print(f"[2] Uploading to s3 ...")
    uploader.upload_to_s3(filepath, filename)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # 3단계: RDS 업로드 (LOAD DATA LOCAL INFILE)
    start = time.time()
    print(f"[3] LOAD DATA LOCAL INFILE로 RDS에 삽입 중...")
    uploader.load_csv_with_local_infile(filepath, TABLE_NAME)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

