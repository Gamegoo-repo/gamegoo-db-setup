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

BOARD_TABLE = 'board'
BOARD_HEADERS = ['member_id', 'board_profile_image','content','game_mode',
                 'mainp','subp','mike','deleted','bump_time','created_at']

BOARD_GAME_STYLE_TABLE = 'board_game_style'
BOARD_GAME_STYLE_HEADERS=['board_id','gamestyle_id','created_at']

BOARD_WANT_POSITION_TABLE = "board_want_positions"
BOARD_WANT_POSITION_HEADERS=['board_id','wantp']

NULL = '\\N'

# board csv 생성
def generate_board_csv(member_ids,rows):
    sampled_member_ids = random.choices(member_ids,k=rows) # member_id rows개 추출
    sorted_dates = rm.generate_sorted_created_at_list(rows,30) #  created_at 추출

    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{BOARD_TABLE}_{rows}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=BOARD_HEADERS)
        writer.writeheader()
        for member_id, base_time in zip(sampled_member_ids,sorted_dates):
            row={ 
                'member_id': member_id,
                'board_profile_image':rm.sample_integer(1,8),
                'content':'\n'.join(fake.paragraphs(nb=3)),
                'game_mode':random.choice(enums.GAMEMODE),
                'mainp':random.choice(enums.POSITION),
                'subp':random.choice(enums.POSITION),
                'mike': random.choice(enums.MIKE),
                'deleted':rm.sample_integer(0,1),
                'bump_time': random.choice([base_time,NULL]),
                'created_at': rm.generate_slots_from_base(base_time,2,6)[1]
            }
            writer.writerow(row)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name, }]

# board_id별 board_game_style csv 생성
def generate_board_game_style_csv(boards):
    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{BOARD_GAME_STYLE_TABLE}_{len(boards)*3}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=BOARD_GAME_STYLE_HEADERS)
        writer.writeheader()
        for (board_id,created_at) in boards:
            sampled_game_style_ids = rm.sample_integers(1,17,3)
            for gamestyle_id in sampled_game_style_ids: # 한 board_id 당 3개의 row
                row={ 
                    'board_id': board_id,
                    'gamestyle_id':gamestyle_id,
                    'created_at':created_at
                }
                writer.writerow(row)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name, }]

# board_id별 board_want_positions csv 생성
def generate_board_want_positions_csv(boards):
    timestamp = datetime.now().strftime("%m%d_%H%M%S")
    file_name = f"{BOARD_WANT_POSITION_TABLE}_{len(boards)*2}r_{timestamp}.csv"
    file_path = f"./csv/{file_name}"

    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=BOARD_WANT_POSITION_HEADERS)
        writer.writeheader()
        for (board_id,created_at) in boards:
            sampled_want_positions = random.sample(enums.POSITION,2)
            for want_position in sampled_want_positions: # 한 board_id 당 2개의 row
                row={ 
                    'board_id': board_id,
                    'wantp':want_position
                }
                writer.writerow(row)

    print(f"csv created at {file_path}")
    return [{"filepath": file_path, "filename": file_name, }]

def run(**kwargs):
    rows = kwargs.get("rows")

    # step 1: member id 조회
    print(f"[1] Fetching member ids...")
    member_ids = [row[0] for row in db_fetcher.fetch_columns("member", ["member_id"])]

    # step 2: board_want_positions 테이블 데이터 초기화 
    print(f"[2] DELETE all rows from {BOARD_WANT_POSITION_TABLE}...")
    db_fetcher.delete_all_rows(BOARD_WANT_POSITION_TABLE)

    # step 3: board_game_style 테이블 데이터 초기화 
    print(f"[3] DELETE all rows from {BOARD_GAME_STYLE_TABLE}...")
    db_fetcher.delete_all_rows(BOARD_GAME_STYLE_TABLE)

    # step 4: board 테이블 데이터 초기화 
    print(f"[4] DELETE all rows from {BOARD_TABLE}...")
    db_fetcher.delete_all_rows(BOARD_TABLE)

    # step 5: board csv 생성
    print(f"[5] Generating {rows} board rows...")
    start = time.time()
    try:
        generated = generate_board_csv(member_ids,rows)
    except ValueError as ve:
        print(f"Error occuerd while generate_board_csv, {ve}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")

    board_filepath = generated[0]["filepath"]
    board_filename = generated[0]["filename"]

    # step 6: board RDS 업로드
    print(f"[6] LOAD DATA LOCAL INFILE - {board_filename} to RDS...")
    start = time.time()
    uploader.load_csv_with_local_infile(board_filepath, BOARD_TABLE)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 7: board id 및 created_at 필드 조회
    print(f"[7] Fetching board...")
    boards = [[row[0],row[1]] for row in db_fetcher.fetch_columns(BOARD_TABLE, ["board_id","created_at"])]

    # step 8: board_game_style csv 생성
    print(f"[8] Generating {rows} * 3 board_game_style rows...")
    start = time.time()
    try:
        generated = generate_board_game_style_csv(boards)
    except Exception as error:
        print(f"Error occuerd while generate_manner_rating_keyword_csv, {error}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")
    
    game_style_filepath = generated[0]["filepath"]
    game_style_filename = generated[0]["filename"]

    # step 9: board_want_positions csv 생성
    print(f"[9] Generating {rows} * 2 board_want_positions rows...")
    start = time.time()
    try:
        generated = generate_board_want_positions_csv(boards)
    except Exception as error:
        print(f"Error occuerd while generate_board_want_positions_csv, {error}")
        return
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")
    
    want_position_filepath = generated[0]["filepath"]
    want_position_filename = generated[0]["filename"]

    # step 10: board_game_style RDS 업로드
    print(f"[10] LOAD DATA LOCAL INFILE - {game_style_filename} to RDS...")
    start = time.time()
    uploader.load_csv_with_local_infile(game_style_filepath, BOARD_GAME_STYLE_TABLE)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 11: board_want_positions RDS 업로드
    print(f"[11] INSERT ALL ROWS - {want_position_filename} to RDS...")
    start = time.time()
    uploader.insert_rows_from_csv(want_position_filepath, BOARD_WANT_POSITION_TABLE)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # step 12, 13: S3 업로드
    print(f"[12] Uploading {game_style_filename} to S3...")
    start = time.time()
    uploader.upload_to_s3(game_style_filepath, game_style_filename)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

    print(f"[13] Uploading {want_position_filename} to S3...")
    start = time.time()
    uploader.upload_to_s3(want_position_filepath, want_position_filename)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

