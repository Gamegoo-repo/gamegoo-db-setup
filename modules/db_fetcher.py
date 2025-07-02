import mysql.connector
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

DB_HOST = os.getenv("RDS_HOST")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")
DB_SCHEMA = os.getenv("RDS_DB")

def fetch_columns(table: str, columns: list[str], where: str = "") -> list[tuple]:
    """
    주어진 테이블에서 원하는 컬럼 조회

    :param table: 테이블명
    :param columns: 조회할 컬럼명 리스트
    :param where: 선택적 WHERE 조건 (예: "status = 'ACTIVE'")
    :return: 컬럼 값 튜플들의 리스트
    """
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_SCHEMA,
    )
    cursor = conn.cursor()

    column_str = ", ".join([f"`{col}`" for col in columns])
    sql = f"SELECT {column_str} FROM `{table}`"
    if where:
        sql += f" WHERE {where}"

    cursor.execute(sql)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    return rows

# 해당 테이블의 모든 row 삭제
def delete_all_rows(table_name):
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_SCHEMA
    )
    cursor = conn.cursor()

    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    # print("☑️ 외래키 제약 조건 비활성화")
    conn.commit()

    cursor.execute(f"DELETE FROM `{table_name}`")
    conn.commit()

    # print("☑️ 외래키 제약 조건 복구")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    conn.commit()

    cursor.close()
    conn.close()

# SELECT * FROM custom query
def fetch_query(sql: str) -> list[tuple]:
    """
    자유로운 SELECT 쿼리를 실행하고 결과 반환
    """
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_SCHEMA,
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

# 커넥션과 커서 반환
def get_connection_and_cursor():
    """
    RDS에 연결된 connection과 cursor를 반환
    :return: (conn, cursor)
    """
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_SCHEMA
    )
    cursor = conn.cursor()
    return conn, cursor