import importlib
import time
import argparse
from modules import uploader as uploader

# 테이블명에 따라 generate py 파일 동적으로 import 및 실행
def generate_csv(table_name, count):
    try:
        module_name = f'generate.generate_{table_name}'
        generator_module = importlib.import_module(module_name) # 동적으로 모듈 import
        file_path,file_name = generator_module.generate(table_name, count) # 해당 모듈 실행
        print(f"✅ CSV generated at {file_path}")
        return file_path, file_name
    except ModuleNotFoundError as e:
        print(f"❌ ModuleNotFoundError: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# -------- 실행 --------
if __name__ == "__main__":
    # argparse로 명령줄 인자 받기
    parser = argparse.ArgumentParser(description="CSV 생성 및 S3 업로드 자동화")
    parser.add_argument("--table", required=True, help="테이블명 (예: member)")
    parser.add_argument("--rows", type=int, default=10000, help="생성할 row 수 (기본: 10000)")
    args = parser.parse_args()

    TABLE_NAME = args.table
    ROW_COUNT = args.rows

    total_start = time.time()

    # 1단계: CSV 생성
    start = time.time()
    print(f"[1] Generating CSV for table '{TABLE_NAME}' with {ROW_COUNT} rows...")
    FILE_PATH,FILE_NAME=generate_csv(TABLE_NAME, ROW_COUNT)
    print(f"⏱️ CSV 생성 소요 시간: {time.time() - start:.2f}초\n")

    # 2단계: S3 업로드
    start = time.time()
    print(f"[2] Uploading to s3://gamegoo/db_data/{FILE_NAME} ...")
    uploader.upload_to_s3(FILE_PATH, FILE_NAME)
    print(f"⏱️ S3 업로드 소요 시간: {time.time() - start:.2f}초\n")

    # 3단계: RDS 업로드 (LOAD DATA LOCAL INFILE)
    start = time.time()
    print(f"[3] LOAD DATA LOCAL INFILE로 RDS에 삽입 중...")
    # uploader.load_csv_with_local_infile(FILE_PATH, TABLE_NAME)
    print(f"⏱️ RDS 업로드 소요 시간: {time.time() - start:.2f}초\n")

    print(f"🧾 전체 소요 시간: {time.time() - total_start:.2f}초")