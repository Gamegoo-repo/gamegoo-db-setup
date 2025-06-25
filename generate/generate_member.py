import csv
from faker import Faker
from modules import random_modules as rm
import random
from modules import enums
import uuid
from datetime import datetime

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

def generate(**kwargs):
    rows = rows = kwargs.get("rows", 10)
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
    return FILE_PATH, FILE_NAME