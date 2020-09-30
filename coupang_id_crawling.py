# crawling
import pandas as pd
import numpy
from selenium import webdriver
import re
import time
import csv

# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import sys
import pendulum
import requests

# 쿠팡 모델명, ID 뽑기
def get_shoes_info():

    brand_info = {
        'adidas' : '80'
        , 'nike' : '79'
        , 'fila' : '83'
        , 'puma' : '81'
        , 'newbalance' : '84'
        , 'reebok' : '85'
        , 'converse' : '86'
    }

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    # 크롤링한 신발들의 정보를 담을 리스트
    shoes_info = []
    
    for b_name, page in brand_info.items():
        for i in range(1, 10):
            url = 'https://www.coupang.com/np/categories/187365?listSize=120&brand='+page+'&offerCondition=&filterType=&isPriceRange=false&minPrice=&maxPrice=&page='+str(i)+'&channel=user&fromComponent=N&selectedPlpKeepFilter=&sorter=bestAsc&filter=&rating=0'
            driver.get(url)
            driver.implicitly_wait(4)
            model_ids = driver.find_elements_by_xpath('/html/body/div[2]/section/form/div/div/div[1]/div/ul/li')
            model_names = driver.find_elements_by_xpath('/html/body/div[2]/section/form/div/div/div[1]/div/ul/li/a/dl/dd/div[2]')
            for q, w in zip(model_ids, model_names):
                model_id = q.get_attribute('id')
                model_name = w.text
                shoes_info.append([b_name, model_id, model_name])

    filename = '/root/coupang_model_id.csv'
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['brand', 'model_id', 'model_name'])
    for i in shoes_info:
        csvWriter.writerow(i)
    f.close()
    
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
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='coupang_id_crawling'
    # DAG 설정을 넣어줌
    , default_args=default_args
    # 최대 실행 횟수
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(days=7)
)
# 크롤링 시작 알림
start_notify = PythonOperator(
    task_id='start_notify',
    python_callable=notify,
    op_kwargs={'context':'쿠팡 모델명 크롤링을 시작하였습니다.'},
    queue='qmaria',
    dag=dag
)
# 크롤링 코드 동작
crawling_code = PythonOperator(
    task_id='id_crawling',
    python_callable=get_shoes_info,
    queue='qmaria',
    dag=dag
)
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'쿠팡 모델명 크롤링이 종료되었습니다.'},
    queue='qmaria',
    dag=dag
)

# 실행 순서 설정
start_notify >> crawling_code >> end_notify
