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
import pymysql

# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import sys
import pendulum
import requests

#--------------------------------실행 횟수 설정----------------------------------#

def get_danawa_brand_count():
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:
            select_count = """
                SELECT count(*) from danawa_brand;
            """
            curs.execute(select_count)
            count = curs.fetchone()[0]

    finally:
        conn.close()

    return count

#--------------------------------크롤링 코드----------------------------------#

def get_shoes_review(b_name, prod_ids):

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)

    for prod_id in prod_ids:
        danawa_reviews = []
        page = 0
        while True:
            page = page + 1
            url = 'http://prod.danawa.com/info/dpg/ajax/companyProductReview.ajax.php?t=0.10499996477784657&prodCode='+str(prod_id)+'&cate1Code=1824&page='+str(page)+'&limit=100&score=0&sortType=&usefullScore=Y&innerKeyword=&subjectWord=0&subjectWordString=&subjectSimilarWordString=&_=1600608005961'
            driver.get(url)
            time.sleep(1)
            driver.implicitly_wait(10)
            rvw_date = driver.find_elements_by_xpath('/html/body/div/div[3]/div[2]/ul/li/div[1]/span[2]')
            rvw_list = driver.find_elements_by_xpath('/html/body/div/div[3]/div[2]/ul/li/div[2]/div[1]/div[2]')

            try:
                no_data = driver.find_element_by_class_name('no_data')
                if no_data != None:
                    break
            except:
                pass
            for q,w in zip(rvw_date,rvw_list):
                filename ='/home/reviews/danawa.txt'
                f = open(filename, 'a', encoding='utf-8', newline='')
                review_date = q.text
                review = w.text
                f.write(f'{prod_id} {review_date} {review}\n')
                f.close()
                danawa_reviews.append([prod_id,review_date,review])
        # 확인 및 백업을 위해 로컬에 csv파일로 저장
        csvwriter = csv.writer(f)
        filename = f'/root/reviews/danawa_{prod_id}.csv'
        f = open(filename, 'w', encoding='utf-8', newline='')
        csvwriter.writerow(['danawa_id','review_date','review'])
        for i in danawa_reviews:
            csvwriter.writerow(i)
        f.close()
    driver.close()

def get_b_name_prod_ids():
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')
    try:
        with conn.cursor() as curs:
            try:
                create_seq = """
                    CREATE SEQUENCE seq_danawa_id START WITH 1 INCREMENT BY 1;
                """
                curs.execute(create_seq)
            except:
                pass

            nextval = """
                SELECT NEXTVAL(seq_danawa_id);
            """
            curs.execute(nextval)
            next_val = curs.fetchone()[0]

            try:
                select_brand = """
                    SELECT brand
                      FROM danawa_brand
                     WHERE idx=%s;
                """
                curs.execute(select_brand, next_val)
                b_name = curs.fetchone()[0]

            except:
                drop_seq = """
                            DROP SEQUENCE seq_danawa_id;
                        """
                curs.execute(drop_seq)

            select_danawa_id = """
                SELECT danawa_id
                  FROM danawa_shoes
                 WHERE brand=%s;
            """
            curs.execute(select_danawa_id, b_name)
            ids = curs.fetchall()

            prod_ids = []
            for i in range(0, len(ids)):
                prod_ids.append(ids[i][0])

    finally:
        conn.close()

    get_shoes_review(b_name, prod_ids)

#--------------------------------에어 플로우 코드----------------------------------#

# 서울 시간 기준으로 변경
local_tz = pendulum.timezone('Asia/Seoul')

# airflow DAG설정        
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 20, tzinfo=local_tz),
    'catchup': False,
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
    , schedule_interval=timedelta(minutes=5)
)

# id 크롤링 종료 감지
start_notify_sensor = ExternalTaskSensor(
      task_id='external_sensor'
    , external_dag_id='line_notify_review_crawling'
    , external_task_id='review_start_notify'
    , mode='reschedule'
    , dag=dag
)

# DAG 동적 생성
count = get_danawa_brand_count()

for count in range(0, count):
    review_crawling = PythonOperator(
        task_id='{0}_review_crawling'.format(count),
        python_callable=get_b_name_prod_ids,
        dag=dag
    )
    start_notify_sensor >> review_crawling
    
    