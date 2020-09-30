# crawling
from bs4 import BeautifulSoup
from selenium import webdriver
import re
import time
import csv
import pandas as pd
import numpy as np

# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import sys
import pendulum
import requests

def get_shoes_review():

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)

    danawa_model_id_path = f'./damawa_model_id.csv'
    model_dataframe = pd.read_csv(danawa_model_id_path)
    model_ids = model_dataframe['model_id']
    
    danawa_reviews = []
    
    for model_id in model_ids:
        page_num = 0

        while True:
            try:
                page_num = page_num+1
                url = 'http://prod.danawa.com/info/dpg/ajax/companyProductReview.ajax.php?t=0.10499996477784657&prodCode='+str(modelId)+'&cate1Code=1824&page='+str(page_num)+'&limit=100&score=0&sortType=&usefullScore=Y&innerKeyword=&subjectWord=0&subjectWordString=&subjectSimilarWordString=&_=1600608005961'
                driver.get(url)
                time.sleep(3)
                review_date = driver.find_elements_by_xpath('/html/body/div/div[3]/div[2]/ul/li/div[1]/span[2]')
                reviews = driver.find_elements_by_xpath('/html/body/div/div[3]/div[2]/ul/li/div[2]/div[1]/div[2]')
                no_data = driver.find_element_by_class_name('no_data')
                if no_data != None:
                    break
            except:
                pass
            for q,w in zip(review_date,reviews):
                danawa_reviews.append([model_id, q.text, w.text])

    filename ='danawa_reviews.csv'
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['model_id','review_date','reviews'])
    for i in danawa_reviews:
        csvWriter.writerow(i)
    f.close()
    driver.close()

# 입력받은 context를 라인으로 메시지 보내는 함수
def notify(context, *args): 
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
      dag_id='danawa_review_crawling'
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
    op_args='다나와 리뷰 크롤링을 시작하였습니다.',
    queue='qmaria',
    dag=dag
)
# 크롤링 코드 동작
crawling_code = PythonOperator(
    task_id='review_crawling',
    python_callable=get_shoes_review,
    queue='qmaria',
    dag=dag
)
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_args='다나와 리뷰 크롤링이 종료되었습니다.',
    queue='qmaria',
    dag=dag
)

# id 크롤링 종료 감지
sensor = ExternalTaskSensor(
      task_id='external_sensor'
    , external_dag_id='danawa_id_crawling'
    , external_task_id='end_notify'
    , execution_date_fn=lambda dt: dt + timedelta(minutes=1)
    , dag=dag
)


# 실행 순서 설정
sensor >> start_notify >> crawling_code >> end_notify