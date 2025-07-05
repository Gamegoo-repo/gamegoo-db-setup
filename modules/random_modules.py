import random
import string
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

def generate_random_string(length):
    characters = string.ascii_letters + string.digits  # a-zA-Z0-9
    return ''.join(random.choices(characters, k=length))

# start ~ end까지의 정수 중 n개 랜덤 선택
def sample_integers(start, end,n):
    if n > end:
        raise ValueError("n은 end보다 클 수 없습니다.")
    return random.sample(range(start, end + 1), n)

# start ~ end까지의 정수 중 1개 랜덤 선택
def sample_integer(start, end):
    return sample_integers(start,end,1)[0]

# start ~ end까지의 실수 중 n개 랜덤 선택
def sample_floats(start, end, n):
    if start > end:
        raise ValueError("start는 end보다 작거나 같아야 합니다.")
    
    # 0.1 단위로 가능한 값 목록 생성
    values = [round(x * 0.1, 1) for x in range(int(start * 10), int(end * 10) + 1)]
    
    if n > len(values):
        raise ValueError("n이 추출 가능한 실수의 개수보다 많습니다.")
    
    return random.sample(values, n)

# start ~ end까지의 실수 중 1개 랜덤 선택
def sample_float(start,end):
    return sample_floats(start,end,1)[0]

def generate_sorted_created_at_list(count, days_range):
    """
    n개의 created_at 값을 datetime 기준으로 정렬된 문자열 리스트로 반환
    """
    created_at_dt_list = [
        fake.date_time_between(start_date=f'-{days_range}d', end_date='now')
        for _ in range(count)
    ]

    # datetime 객체 기준 정렬
    created_at_dt_list.sort()

    # 문자열(datetime(6))로 변환
    return [dt.strftime("%Y-%m-%d %H:%M:%S.%f") for dt in created_at_dt_list]

def generate_sorted_created_at_list_with_period(count, start_days_ago, end_days_ago):
    """
    오늘로부터 start_days_ago ~ end_days_ago 사이의 랜덤 날짜 n개를 정렬된 문자열(datetime(6))로 반환
        count (int): 생성할 개수
        start_days_ago (int): 시작일 (오늘로부터 며칠 전)
        end_days_ago (int): 종료일 (오늘로부터 며칠 전, start보다 작아야 함)
        List[str]: 정렬된 datetime(6) 문자열 리스트
    """
    if start_days_ago < end_days_ago:
        raise ValueError("start_days_ago는 end_days_ago보다 크거나 같아야 합니다.")

    start_date = datetime.now() - timedelta(days=start_days_ago)
    end_date = datetime.now() - timedelta(days=end_days_ago)

    dt_list = [
        fake.date_time_between(start_date=start_date, end_date=end_date)
        for _ in range(count)
    ]

    dt_list.sort()
    return [dt.strftime("%Y-%m-%d %H:%M:%S.%f") for dt in dt_list]

def generate_slots_from_base(base_time_str: str, k: int, gap_minutes: int = 5) -> list[str]:
    """
    주어진 base_time을 기준으로 일정 간격으로 datetime(6) 문자열 k개 생성
    base_time_str (str): 기준 시각, 형식은 '%Y-%m-%d %H:%M:%S.%f'
    k (int): 생성할 datetime 개수
    gap_minutes (int): 간격 (기본 5분)

        List[str]: datetime 문자열 리스트 (오름차순)
    """
    base_dt = datetime.strptime(base_time_str, "%Y-%m-%d %H:%M:%S.%f")
    return [
        (base_dt - timedelta(minutes=i * gap_minutes)).strftime("%Y-%m-%d %H:%M:%S.%f")
        for i in range(k)
    ]

def generate_sorted_after_created_at(base_created_at_str: str, count: int) -> list[str]:
    """
    base_created_at 이후부터 현재까지 중에서 랜덤한 datetime(6) 문자열을 count개 생성하여 정렬 반환
        base_created_at_str (str): 기준 시각 문자열 ('%Y-%m-%d %H:%M:%S.%f')
        count (int): 생성할 개수

        List[str]: 오름차순 정렬된 datetime(6) 문자열 리스트
    """
    base_dt = datetime.strptime(base_created_at_str, "%Y-%m-%d %H:%M:%S.%f")
    now = datetime.now()

    if base_dt >= now:
        raise ValueError("기준 시각은 현재 시각보다 이전이어야 합니다.")

    dt_list = [
        base_dt + (now - base_dt) * random.random()
        for _ in range(count)
    ]

    dt_list.sort()
    return [dt.strftime("%Y-%m-%d %H:%M:%S.%f") for dt in dt_list]

def random_iso8601_datetime(start: str, end: str = None) -> str:
    """
    시작(start) ~ 종료(end) 구간 중 랜덤한 ISO 8601 형식 LocalDateTime 문자열 반환.
    end가 주어지지 않으면 현재 시각을 사용.
    
    :param start: "YYYY-MM-DDTHH:MM:SS" 형식 문자열
    :param end: "YYYY-MM-DDTHH:MM:SS" 형식 문자열 (옵션, default: 현재 시각)
    :return: ISO 8601 형식 문자열
    """
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end) if end else datetime.now()

    delta = end_dt - start_dt
    random_seconds = random.randint(0, int(delta.total_seconds()))
    random_dt = start_dt + timedelta(seconds=random_seconds)

    return random_dt.isoformat(timespec='seconds')