import random
import string
from faker import Faker

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

# n개의 created_at 값을 datetime 기준으로 정렬된 문자열 리스트로 반환
def generate_sorted_created_at_list(count, days_range):
    created_at_dt_list = [
        fake.date_time_between(start_date=f'-{days_range}d', end_date='now')
        for _ in range(count)
    ]

    # datetime 객체 기준 정렬
    created_at_dt_list.sort()

    # 문자열(datetime(6))로 변환
    return [dt.strftime("%Y-%m-%d %H:%M:%S.%f") for dt in created_at_dt_list]
