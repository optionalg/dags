# crawling
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
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

# 신발 정보 가져오는 함수
def get_shoes_full_info():

    # 13876=나이키, 10851=아디다스, 13770=리복, 13760=뉴발란스, 10789=휠라, 12042=푸마, 10719=호킨스, 10986=컨버스, 10720=반스
    brand_page_nubmers = ['13876', '10851', '13770', '13760', '10789', '12042', '10719', '10986', '10720']
    # brand_names = ['nike','adidas','reebok','newbalance','fila','puma','hokins','converse','vans']

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    # 크롤링한 신발들의 정보를 담을 리스트
    shoes_full_info = []
    
    for j in brand_page_nubmers:
        # 리뷰 많은 순으로 정렬하여 20페이지가지만 진행
        for i in range(1,21):
            url = 'http://search.danawa.com/dsearch.php?query=%EC%8B%A0%EB%B0%9C&originalQuery=%EC%8B%A0%EB%B0%9C&previousKeyword=%EC%8B%A0%EB%B0%9C&volumeType=allvs&page='+str(i)+'&limit=120&sort=opinionDESC&list=list&boost=true&addDelivery=N&brand='+str(j)+'&tab=main'
            driver.get(url)
            time.sleep(3)
            try:
                nosearchArea = driver.find_element_by_selector('#nosearchArea')
                print(nosearchArea)
                break
            except:
                pass
            # 모델 코드, 모델 이름, 모델 정보
            model_ids = driver.find_elements_by_class_name('relation_goods_unit')
            model_names = driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div[3]/div[2]/div[7]/div[2]/div[2]/div[3]/ul/li/div/div[2]/p/a')
            model_infos = driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div[3]/div[2]/div[7]/div[2]/div[2]/div[3]/ul/li/div/div[2]/dl/dd/div')
            for q,w,e in zip(model_ids,model_names,model_infos):
                model_id = q.get_attribute('id')[20:]
                model_name = w.text
                brand = w.text.split(sep=' ')[0]
                model_info = e.text
                model_category = e.text.split(sep='/')[1]
                shoes_full_info.append([brand, model_id, model_name, model_category, model_info])
                
    # 브랜드이름 파일명으로 저장
    filename = '/root/reviews/danawa_model_id.csv'
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['brand','model_id','model_name','model_category','model_info'])
    for i in shoes_full_info:
        csvWriter.writerow(i)
    f.close()
    driver.close()

def get_shoes_review():

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    # model_id 불러오기
    danawa_model_id_path = '/root/reviews/danawa_model_id.csv'
    model_dataframe = pd.read_csv(danawa_model_id_path)
    model_ids = model_dataframe['model_id']
    
    danawa_reviews = []
    progress = 0
    
    for model_id in model_ids:
        print(model_id)
        page_num = 0.0

        while True:
            try:
                page_num = page_num+1
                url = 'http://prod.danawa.com/info/dpg/ajax/companyProductReview.ajax.php?t=0.10499996477784657&prodCode='+str(model_id)+'&cate1Code=1824&page='+str(page_num)+'&limit=100&score=0&sortType=&usefullScore=Y&innerKeyword=&subjectWord=0&subjectWordString=&subjectSimilarWordString=&_=1600608005961'
                driver.get(url)
                time.sleep(3)
                review_date = driver.find_elements_by_xpath('/html/body/div/div[3]/div[2]/ul/li/div[1]/span[2]')
                reviews = driver.find_elements_by_xpath('/html/body/div/div[3]/div[2]/ul/li/div[2]/div[1]/div[2]')
                no_data = driver.find_element_by_class_name('no_data')
                if no_data != None:
                    break
            except:
                pass
            for q,w in zip(review_date ,reviews):
                # !!!!!! append 안에다가 .text를 넣으면 centos에서 작동안함 반드시 밖으로 빼서 append 할것!!!!!!
                date = q.text
                review = w.text
                danawa_reviews.append([model_id, date, review])
    
        # 진행상황 체크                
        progress = progress + 1.0
        progress_percent = (progress * 100) / float(len(model_ids))
        progress_check = f'{progress_percent:.2f}% 완료되었습니다.'
        TARGET_URL = 'https://notify-api.line.me/api/notify'
        TOKEN = 'sw0dTqnM0kEiJETNz2aukiTjhzsrIQlmdR0gdbDeSK3'

        # 요청합니다.
        requests.post(
            TARGET_URL
            , headers={
                'Authorization' : 'Bearer ' + TOKEN
            }
            , data={
                'message' : progress_check
            }
        )
        
    filename ='/root/reviews/danawa_reviews.csv'
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['model_id','review_date','reviews'])
    for i in danawa_reviews:
        csvWriter.writerow(i)
    f.close()
    driver.close()

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
    op_kwargs={'context':'다나와 리뷰 크롤링을 시작하였습니다.'},
    queue='q24',
    dag=dag
)
# 크롤링 코드 동작
id_crawling_code = PythonOperator(
    task_id='id_crawling',
    python_callable=get_shoes_full_info,
    queue='q24',
    dag=dag
)

# 크롤링 코드 동작
review_crawling_code = PythonOperator(
    task_id='review_crawling',
    python_callable=get_shoes_review,
    queue='q24',
    dag=dag
)

# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'다나와 리뷰 크롤링이 종료되었습니다.'},
    queue='q24',
    dag=dag
)

# 실행 순서 설정
start_notify >> id_crawling_code >> review_crawling_code >> end_notify