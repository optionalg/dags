# crawling
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import re
import time
import csv
import pandas as pd
import numpy as np
import datetime as dt

# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import sys
import pendulum
import requests

# 신발 정보 가져오는 함수
def get_shoes_info():

    brand_info = {
          'adidas' : '10851'
        , 'nike' : '13876'
        , 'fila' : '10789'
        , 'puma' : '12042'
        , 'newbalance' : '13760'
        , 'reebok' : '13770'
        , 'converse' : '10986'
        , 'hokins' : '10719'
        , 'vans' : '10720'
        , 'crocs' : '10828'
        , 'superga' : '10750'
    }

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    # 크롤링한 신발들의 정보를 담을 리스트
    shoes_full_info = []
    
    for b_name, page in brand_info.items():
        # 리뷰 많은 순으로 정렬하여 20페이지까지만 진행
        for i in range(1,21):
            url = 'http://search.danawa.com/dsearch.php?query=%EC%8B%A0%EB%B0%9C&originalQuery=%EC%8B%A0%EB%B0%9C&previousKeyword=%EC%8B%A0%EB%B0%9C&volumeType=allvs&page='+str(i)+'&limit=120&sort=opinionDESC&list=list&boost=true&addDelivery=N&brand='+str(page)+'&tab=main'
            driver.get(url)
            time.sleep(3)
            try:
                nosearchArea = driver.find_element_by_selector('#nosearchArea')
                print(nosearchArea)
                break
            except:
                pass
            # 모델 코드, 모델 이름, 모델 정보
            prod_ids = driver.find_elements_by_class_name('relation_goods_unit')
            prod_names = driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div[3]/div[2]/div[7]/div[2]/div[2]/div[3]/ul/li/div/div[2]/p/a')
            prod_infos = driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div[3]/div[2]/div[7]/div[2]/div[2]/div[3]/ul/li/div/div[2]/dl/dd/div')
            for q,w,e in zip(prod_ids,prod_names,prod_infos):
                prod_id = q.get_attribute('id')[20:]
                prod_name = w.text
                prod_info = e.text
                prod_category = e.text.split(sep='/')[1]
                shoes_full_info.append([b_name, prod_id, prod_name, prod_category, prod_info])
                
    # 브랜드이름 파일명으로 저장
    now = dt.datetime.now()
    nowDate = now.strftime('%Y_%m_%d')
    filename = '/root/reviews/danawa_prod_id_{}.csv'.format(nowDate)
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['brand','prod_id','prod_name','prod_category','prod_info'])
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
    
    # prod_id 불러오기
    now = dt.datetime.now()
    nowDate = now.strftime('%Y_%m_%d')
    danawa_prod_id_path = '/root/reviews/danawa_prod_id_{}.csv'.format(nowDate)
    prod_dataframe = pd.read_csv(danawa_prod_id_path)
    prod_ids = prod_dataframe['prod_id']
    
    danawa_reviews = []
    img_url_list = []
    
    progress = 0
    progress_check = 0
    
    for prod_id in prod_ids:
        print(prod_id)
        page_num = 0.0

        while True:
            page = page + 1
            url = 'http://prod.danawa.com/info/dpg/ajax/companyProductReview.ajax.php?t=0.10499996477784657&prodCode='+str(modelId)+'&cate1Code=1824&page='+str(page)+'&limit=100&score=0&sortType=&usefullScore=Y&innerKeyword=&subjectWord=0&subjectWordString=&subjectSimilarWordString=&_=1600608005961'
            driver.get(url)
            time.sleep(3)
            rvw_date = driver.find_elements_by_xpath('/html/body/div/div[3]/div[2]/ul/li/div[1]/span[2]')
            rvw_list = driver.find_elements_by_xpath('/html/body/div/div[3]/div[2]/ul/li/div[2]/div[1]/div[2]')
            danawa_img_list = driver.find_elements_by_class_name('center > img')
            for img_src in danawa_img_list:
                danawa_img_url = img_src.get_attribute('src')
                img_url_list.append(danawa_img_url)
            try:
                no_data = driver.find_element_by_class_name('no_data')
                if no_data != None:
                    break
            except:
                pass
            for q,w in zip(rvw_date,rvw_list):
                danawa_rvws.append([q.text,w.text,modelId])
        n = 1
        for url in img_url_list:
            n = n + 1
            r = requests.get(url)
            file = open(f'/root/images/danawa_{prod_id}_{n}.jpg', 'wb')
            file.write(r.content)
            file.close()
    
        # 진행상황 체크                
        progress = progress + 1
        progress_percent = round((progress * 100) / float(len(prod_ids)))
        if progress_check < progress_percent :
            progress_check = progress_percent
            progress_notify = f'다나와 리뷰 크롤링 {str(progress_percent)}% 완료되었습니다.'
            TARGET_URL = 'https://notify-api.line.me/api/notify'
            TOKEN = 'sw0dTqnM0kEiJETNz2aukiTjhzsrIQlmdR0gdbDeSK3'

            # 요청합니다.
            requests.post(
                TARGET_URL
                , headers={
                    'Authorization' : 'Bearer ' + TOKEN
                }
                , data={
                    'message' : progress_notify
                }
            )

        
    filename ='/root/reviews/danawa_reviews_{}.csv'.format(nowDate)
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['prod_id','review_date','reviews'])
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
    'catchup': False,
    'retries': 1,
    'retry_delay':timedelta(minutes=1)
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
    op_kwargs={'context':'다나와 크롤링을 시작하였습니다.'},
    queue='q23',
    dag=dag
)
# id 크롤링
id_crawling_code = PythonOperator(
    task_id='id_crawling',
    python_callable=get_shoes_info,
    queue='q23',
    dag=dag
)
# 리뷰 크롤링
review_crawling_code = PythonOperator(
    task_id='review_crawling',
    python_callable=get_shoes_review,
    queue='q23',
    dag=dag
)
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'다나와 크롤링이 종료되었습니다.'},
    queue='q23',
    dag=dag
)

# 실행 순서 설정
start_notify >> id_crawling_code >> review_crawling_code >> end_notify