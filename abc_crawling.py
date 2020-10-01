# shared
import requests

# crawling
from bs4 import BeautifulSoup
from selenium import webdriver
import re
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
import time
import csv

# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import pendulum

# 가져올 카테고리 리스트
category_name = ['sneakers','sports','shoes','sandals','boots']

#abc마트에서 신발 모델들을 가져오는 함수
def get_shoe_model_list():
    # 운동화,스포츠,구두,센들,부츠 페이지번호
    category_number =['1000000245','1000000249','1000000254','1000000260','1000000266']
    name_seq = 0
    # 각 카테고리 별로 실행
    for i in category_number:
        temp_model_list = []    
        
        #각 항목별 16페이지까지 진행
        for page in range(1,17):
            # 크롬 드라이버 옵션
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-gpu')
            # 옵션을 넣어 드라이버 생성
            driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
            # url을 넣어 얻어옴
            url = 'https://abcmart.a-rt.com/display/search-word/result/list?searchPageType=category&ctgrNo='+str(i)+'&brand=000003&brand=000074&brand=000125&brand=000032&brand=000050&brand=000081&brand=000048&brand=000054&brand=000058&brand=000072&chnnlNo=10001&ctgrLevel=1&leafCtgrYn=N&pageColumn=4&sort=latest&perPage=100&rdoProdGridModule=col4&resultExistSmartFilter=Y&page='+str(page)+'&_=1600341975816'
            driver.get(url)
            # 페이지 불러오기위해 5초간 대기
            time.sleep(5)
            # 브랜드, 신발명, 가격 가져오기
            brand = driver.find_elements_by_class_name('prod-brand')
            name = driver.find_elements_by_class_name('prod-name')
            cost = driver.find_elements_by_class_name('price-cost')
            # zip함수로 하나로 묶어서 임시 리스트에 넣기
            for brand_text,name_text,cost_text in zip(brand, name, cost):
                temp_model_list.append([
                      brand_text.text
                    , name_text.text
                    , cost_text.text])
            # 드라이버 클로즈
            driver.close()
        
        # csv파일 만들어서 오픈
        f = open('/root/reviews/{0}.csv'.format(category_name[name_seq]), 'w', encoding='utf-8', newline='')
        # csv 형식으로 쓰기
        csvWriter = csv.writer(f)
        # 헤드 생성
        csvWriter.writerow(['brand','prod-name','price-cost','category'])
        # 임시 리스트에서 하나씩 꺼내와서 쓰기
        for model in temp_model_list:
            csvWriter.writerow(model)
        # 파일 닫기
        f.close()
        name_seq = name_seq + 1

# 입력받은 context를 라인으로 메시지 보내는 함수
def notify(context, **kwargs): 
    TARGET_URL = 'https://notify-api.line.me/api/notify'
    TOKEN = 'sw0dTqnM0kEiJETNz2aukiTjhzsrIQlmdR0gdbDeSK3'

    # 요청합니다.
    requests.post(
          TARGET_URL
        , headers={
            'Authorization' : 'Bearer ' + TOKEN
        }
        , data={
            'message' : context
        }
    )
# 서울 시간 기준으로 변경
local_tz = pendulum.timezone('Asia/Seoul')

# airflow DAG설정        
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 30, tzinfo=local_tz),
    'catchup': False
}    
    
# DAG인스턴스 생성
dag = DAG(
      dag_id='abc_model_crawling'
    # DAG 설정을 넣어줌
    , default_args=default_args
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(days=7)
)
# 크롤링 시작 알림
start_notify = PythonOperator(
    task_id='start_notify',
    python_callable=notify,
    op_kwargs={'context':'abc 모델명 크롤링을 시작하였습니다.'},
    queue='q22',
    dag=dag
)
# 크롤링 코드 동작
crawling_code = PythonOperator(
    task_id='crawling_code',
    python_callable=get_shoe_model_list,
    queue='q22',
    dag=dag
)
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'abc 모델명 크롤링이 종료되었습니다.'},
    queue='q22',
    dag=dag
)

# 실행 순서 설정
start_notify >> crawling_code >> end_notify

# 다음과 같이도 가능하다 - set_downstream
# start_notify.set_downstream(crawling_code)
# crawling_code.set_downstream(end_notify)

# 다음과 같이도 가능하다 - set_upstream
# crawling_code.set_upstream(start_notify)
# end_notify.set_upstream(crawling_code)


    
