import importlib
import time
import argparse
from modules import uploader as uploader
from arg_config import build_parser 

# 테이블명에 따라 generate py 파일 동적으로 import 및 실행
def generate_csv(table_name, **kwargs):
    try:
        module_name = f'generate.generate_{table_name}'
        generator_module = importlib.import_module(module_name) # 동적으로 모듈 import
        file_path,file_name = generator_module.generate(**kwargs) # 해당 모듈 실행
        print(f"✅ CSV generated at {file_path}")
        return file_path, file_name
    except ModuleNotFoundError as e:
        print(f"❌ ModuleNotFoundError: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# -------- 실행 --------
if __name__ == "__main__":
    # argparse로 명령줄 인자 받기
    parser = build_parser()
    args = parser.parse_args()

    TABLE_NAME = args.table
    arg_dict = vars(args)
    arg_dict.pop("table")  # generate 함수에는 table 인자 제외

    total_start = time.time()

    # 1단계: CSV 생성
    start = time.time()
    print(f"[1] Generating CSV for table '{TABLE_NAME}'...")
    FILE_PATH,FILE_NAME=generate_csv(TABLE_NAME, **arg_dict)
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")

    # # 2단계: S3 업로드
    # start = time.time()
    # print(f"[2] Uploading to s3://gamegoo/db_data/{FILE_NAME} ...")
    # uploader.upload_to_s3(FILE_PATH, FILE_NAME)
    # print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # # 3단계: RDS 업로드 (LOAD DATA LOCAL INFILE)
    # start = time.time()
    # print(f"[3] LOAD DATA LOCAL INFILE로 RDS에 삽입 중...")
    # # uploader.load_csv_with_local_infile(FILE_PATH, TABLE_NAME)
    # print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

    print(f"🧾 전체 소요 시간: {time.time() - total_start:.2f}초")