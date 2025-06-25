import importlib
import time
import argparse
from modules import uploader as uploader
from arg_config import build_parser 

# 테이블명에 따라 generate py 파일 동적으로 import 및 실행
def run_module(table_name, **kwargs):
    try:
        module_name = f'generate.generate_{table_name}'
        generator_module = importlib.import_module(module_name) # 동적으로 모듈 import
        generator_module.run(**kwargs) # 해당 모듈 실행
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
    arg_dict.pop("table")  

    print(f"-- Running generation flow for table: {TABLE_NAME} --")
    total_start = time.time()
    run_module(TABLE_NAME, **arg_dict)
    print(f"-- 🧾 전체 소요 시간: {time.time() - total_start:.2f}초 --")
