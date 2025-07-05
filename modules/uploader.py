import mysql.connector
import os
import boto3
import json
import csv
from dotenv import load_dotenv
from datetime import datetime
from modules import primary_key

# .env 파일 로드
load_dotenv()

# 환경변수
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
DB_HOST = os.getenv("RDS_HOST")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")
DB_SCHEMA = os.getenv("RDS_DB")
S3_BUCKET = 'gamegoo'

# boto3 클라이언트 생성
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# s3 버킷에 파일 업로드
def upload_to_s3(filepath, filename):
    S3_KEY=f"db_data/{filename}"
    s3.upload_file(filepath, S3_BUCKET, S3_KEY)
    print("✅ s3 Upload complete.")

# 해당 테이블의 모든 인덱스 조회
def get_index_definitions(cursor, table_name):
    cursor.execute(f"""
        SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY INDEX_NAME, SEQ_IN_INDEX;
    """, (DB_SCHEMA, table_name))

    indexes = {}
    for index_name, column_name, seq, non_unique in cursor.fetchall():
        if index_name not in indexes:
            indexes[index_name] = {'columns': [], 'non_unique': non_unique}
        indexes[index_name]['columns'].append(column_name)
    print(f"☑️ 인덱스 조회 완료: {table_name}")
    return indexes

# DROP 대상 인덱스 조회
def get_safe_indexes_to_drop(cursor, table_name):
    safe_indexes={}
    try:
        full_indexes = {}
        indexes_to_exclude = set()
        auto_increment_column = {}
        
        if table_name in primary_key.DICT:
            auto_increment_column = primary_key.DICT[table_name]

        # 🔹 외래키 제약조건에 사용되는 인덱스 조회 및 제외 처리
        cursor.execute("""
            SELECT DISTINCT s.INDEX_NAME
            FROM information_schema.KEY_COLUMN_USAGE k
            JOIN information_schema.STATISTICS s
            ON k.TABLE_SCHEMA = s.TABLE_SCHEMA
            AND k.TABLE_NAME = s.TABLE_NAME
            AND k.COLUMN_NAME = s.COLUMN_NAME
            WHERE k.TABLE_SCHEMA = %s
                AND k.TABLE_NAME = %s
                AND k.REFERENCED_TABLE_NAME IS NOT NULL;
        """, (DB_SCHEMA, table_name))
        fk_index_names = {row[0] for row in cursor.fetchall()}
        indexes_to_exclude.update(fk_index_names)

        cursor.execute("""
            SELECT
                s.INDEX_NAME,
                s.COLUMN_NAME,
                s.SEQ_IN_INDEX,
                s.NON_UNIQUE,
                s.INDEX_TYPE,
                s.COMMENT
            FROM information_schema.STATISTICS s
            WHERE s.TABLE_SCHEMA = %s
            AND s.TABLE_NAME = %s
            ORDER BY s.INDEX_NAME, s.SEQ_IN_INDEX;
        """, (DB_SCHEMA, table_name))

        # 인덱스 전체 구성 및 auto_increment 컬럼 관련 인덱스 추출
        for index_name, column_name, seq, non_unique, index_type, comment in cursor.fetchall():
            if index_name not in full_indexes:
                full_indexes[index_name] = {
                    'columns': [], # (col_name, seq)
                    'non_unique': non_unique,
                    'index_type': index_type,
                    'comment': comment
                }
            full_indexes[index_name]['columns'].append((column_name, seq))

            if column_name == auto_increment_column:
                indexes_to_exclude.add(index_name)

        # 정렬 후 column 이름만 리스트로 변환
        # 복합 인덱스 column 순서 반영
        for idx_info in full_indexes.values():
            idx_info['columns'] = [
                col for col, _ in sorted(idx_info['columns'], key=lambda x: x[1])
            ]

        # PRIMARY와 auto_increment, FK 포함 인덱스 제외
        safe_indexes = {
            name: info
            for name, info in full_indexes.items()
            if name != 'PRIMARY' and name not in indexes_to_exclude
        }
    except Exception as error:
        print(f"❌ DROP 가능한 인덱스 조회 중 오류 발생: {error}")

    print(f"☑️ DROP 가능한 인덱스 개수: {len(safe_indexes)}")
    return safe_indexes

# 인덱스 백업 파일 생성
def save_index_backup(indexes, table_name):
    try:
        timestamp = datetime.now().strftime("%m%d_%H%M%S")
        FILE_NAME = f"./index_backup/{table_name}_index_backup_{timestamp}.json"
        with open(FILE_NAME, 'w') as f:
            json.dump(indexes, f, indent=2)
    except Exception as error:
        print(f"❌ 인덱스 백업 파일 저장 중 오류 발생: {error}")
    
    print(f"☑️ 인덱스 백업 저장: {FILE_NAME}")
    return FILE_NAME

# 해당 테이블의 모든 인덱스 제거
def drop_indexes(cursor, table_name, indexes):
    for index_name in indexes:
        if index_name == 'PRIMARY':
            cursor.execute(f"ALTER TABLE `{table_name}` DROP PRIMARY KEY;")
        else:
            cursor.execute(f"ALTER TABLE `{table_name}` DROP INDEX `{index_name}`;")
    print("☑️ 제거 대상 인덱스 제거 완료")

# 해당 테이블의 모든 인덱스 복구
def recreate_indexes(cursor, table_name, indexes):
    for name, info in indexes.items():
        cols = ', '.join(info['columns'])
        if name == 'PRIMARY':
            cursor.execute(f"ALTER TABLE `{table_name}` ADD PRIMARY KEY ({cols});")
        else:
            unique = 'UNIQUE ' if info['non_unique'] == 0 else ''
            cursor.execute(f"ALTER TABLE `{table_name}` ADD {unique}INDEX `{name}` ({cols});")
    print("☑️ 인덱스 복구 완료")

# rds에 데이터 로드
def load_data_local_infile(cursor,filepath,table_name):
    columns_str = ""
    # 1. 헤더 추출
    with open(filepath, newline='') as f:
        reader = csv.reader(f)
        headers = next(reader)  # 첫 줄 읽기

        # 2. 컬럼 리스트 문자열 생성
        columns_str = ', '.join([f'`{col.strip()}`' for col in headers])

    print(f"📥 LOAD DATA LOCAL INFILE 실행 중...")
    sql = f"""
        LOAD DATA LOCAL INFILE '{filepath}'
        INTO TABLE {table_name}
        FIELDS TERMINATED BY ',' ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
        ({columns_str});
    """

    cursor.execute(sql)


# rds에 csv 파일 LOAD DATA
def load_csv_with_local_infile(filepath, table_name):
    ABS_PATH = os.path.abspath(filepath)
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_SCHEMA,
            allow_local_infile=True  
        )
    except mysql.connector.Error as conn_err:
        print(f"❌ RDS 연결 실패: {conn_err}")
        return
    
    cursor = conn.cursor()
    indexes = {}
    inserted_count = 0
    backup_file_name=""

    try:
        # 삽입 전 row 수
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        before_count = cursor.fetchone()[0]

        
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        print("☑️ 외래키 제약 조건 비활성화")

        indexes = get_safe_indexes_to_drop(cursor, table_name)
        if indexes:
            backup_file_name = save_index_backup(indexes, table_name)
            drop_indexes(cursor, table_name, indexes)
            conn.commit()

        # rds에 데이터 로드
        load_data_local_infile(cursor,ABS_PATH,table_name)
        conn.commit()

        # 삽입 후 row 수
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        after_count = cursor.fetchone()[0]
        inserted_count = after_count - before_count
    
    except mysql.connector.Error as e:
        print(f"❌ 삽입 중 오류 발생: {e}")
        print("🛠 인덱스 복구 시도 중...")
        try:
            recreate_indexes(cursor, table_name, indexes)
            conn.commit()
            print("✅ 인덱스 복구 성공")
        except Exception as rec_err:
            print(f"❌ 인덱스 복구 실패: {rec_err}")
        finally:
            print("☑️ 외래키 제약 조건 복구")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            conn.commit()
            cursor.close()
            conn.close()
        print("⚠️ 삽입 실패. 인덱스 백업 파일:", backup_file_name)
        return
    
    try:
        if indexes:
            print("🔁 인덱스 복구 중...")
            recreate_indexes(cursor, table_name, indexes)
            conn.commit()

        print("☑️ 외래키 제약 조건 복구")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        conn.commit()
    
    except mysql.connector.Error as post_err:
        print(f"⚠️ 삽입은 성공했지만 인덱스 복구 중 오류 발생: {post_err}")
        print("백업 파일:", backup_file_name)

    cursor.close()
    conn.close()
    print(f"✅ RDS upload complete. TABLE: {table_name}, {inserted_count} rows inserted.")

# row 마다 개별 insert
def insert_rows_from_csv(filepath,table_name):
    # 1. DB 연결
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_SCHEMA
    )
    cursor = conn.cursor()

    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames  # 첫 줄의 필드명 리스트
        column_str = ', '.join(f"`{col}`" for col in headers)
        placeholder_str = ', '.join(['%s'] * len(headers))

        sql = f"""
            INSERT INTO `{table_name}` ({column_str})
            VALUES ({placeholder_str})
        """

        inserted = 0
        for row in reader:
            values = [row[col].strip() if row[col] != "" else None for col in headers]
            cursor.execute(sql, values)
            inserted+=1

    conn.commit()
    print(f"✅ INSERT 완료: {inserted} rows inserted into `{table_name}`.")
    cursor.close()
    conn.close()


def insert_rows_from_csv_batch(filepath, table_name, batch_size=1000):
    # 1. DB 연결
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_SCHEMA
    )
    cursor = conn.cursor()

    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames  # 첫 줄의 필드명 리스트
        column_str = ', '.join(f"`{col}`" for col in headers)
        placeholder_str = ', '.join(['%s'] * len(headers))

        sql = f"""
            INSERT INTO `{table_name}` ({column_str})
            VALUES ({placeholder_str})
        """

        batch = []
        total_inserted = 0

        for row in reader:
            values = [row[col].strip() if row[col] != "" else None for col in headers]
            batch.append(values)

            if len(batch) >= batch_size:
                cursor.executemany(sql, batch)
                total_inserted += len(batch)
                batch.clear()
                print("batch inserted")

        # 마지막 남은 데이터
        if batch:
            cursor.executemany(sql, batch)
            total_inserted += len(batch)

    conn.commit()
    print(f"✅ INSERT 완료: {total_inserted} rows inserted into `{table_name}`.")
    cursor.close()
    conn.close()