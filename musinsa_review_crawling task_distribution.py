# crawling
import pandas as pd
import numpy
from selenium import webdriver
import re
import time
import csv
import datetime as dt
import pymysql

# airflow 
from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import pendulum
import requests

def get_musinsa_count():
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:
            select_count = """
                SELECT count(*) from musinsa_shoes;
            """
            curs.execute(select_count)
            count = curs.fetchone()[0]

    finally:
        conn.close()
    count = int(count / 500) + 1
    return count

def get_shoes_review(prod_ids):

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)

    style_list = ['photo','goods']

    for style in style_list:
        for prod_id in prod_ids:
            page_num = 0
            while True:
                page_num = page_num + 1
                url = 'https://store.musinsa.com/app/reviews/goods_estimate_list/'+str(style)+'/'+str(prod_id)+'/0/'+str(page_num)
                driver.get(url)
                time.sleep(1)
                driver.implicitly_wait(10)
                prod_rvw_date = driver.find_elements_by_class_name('date')
                #prod_name = driver.find_elements_by_class_name('list_info.p_name')
                prod_cust_buy_size = driver.find_elements_by_class_name('txt_option')
                prod_size_jud = driver.find_elements_by_css_selector('body > div > div > div > div.postRight > div > div.prd-level-each > ul')
                prod_rvw = driver.find_elements_by_class_name('content-review')
                
                try:
                    no_data = driver.find_element_by_class_name('mypage_review_none')
                    if no_data != None:
                        break
                except:
                    pass
                    
                size = []
                footwidth = []
                ignition = []
                for prod_size_jud_split in prod_size_jud:
                    prod_size_jud_text = prod_size_jud_split.text
                    try:
                        test = prod_size_jud_text.split('\n')
                        size.append(test[0])
                        footwidth.append(test[3])
                        ignition.append(test[4])
                    except:
                        pass
                        
                for q,e,r,si,fo,ig in zip(prod_rvw_date,prod_cust_buy_size,prod_rvw,size,footwidth,ignition):
                    review_date = q.text
                    buy_size = e.text
                    review = r.text
                    review = review.replace('\n','')
                    if re.search('보', si) : si = 0
                    elif re.search('커', si) : si = 1
                    else : si = -1 
                    if re.search('보', fo) : fo = 0
                    elif re.search('넓', fo) : fo = 1
                    else : fo = -1 
                    if re.search('적', ig) : ig = 0
                    elif re.search('편', ig) : ig = 1
                    else : ig = -1 
                    
                    filename = f'/home/reviews/musinsa.txt'
                    f = open(filename, 'a', encoding='utf-8', newline='')
                    f.write(f'{prod_id} {review_date[:10]} {buy_size[1:4]} {si} {fo} {ig} {review}\n')
                    f.close()
    driver.close()

def get_category_prod_ids():
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')
    try:
        with conn.cursor() as curs:
            select_musinsa_id = """
                SELECT musinsa_id
                  FROM musinsa_shoes;
            """
            curs.execute(select_musinsa_id)
            ids = curs.fetchall()
    finally:
        conn.close()
        
    return ids


def distribute_task(ids):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')
    try:
        with conn.cursor as curs:
            prod_ids_all = []
            for i in range(0, len(ids)):
                prod_ids_all.append(ids[i][0])

            try:
                create_seq = """
                    CREATE SEQUENCE seq_task_unit INCREMENT BY 500 MINVALUE 0;
                """
                curs.execute(create_seq)

                create_seq2 = """
                    CREATE SEQUENCE seq_task_unit2 INCREMENT BY 500 MINVALUE 500;
                """
                curs.execute(create_seq2)
            except:
                pass

            select_start_point = """
                SELECT NEXTVAL(seq_task_unit)
            """
            curs.execute(select_start_point)
            start_point = curs.fetchone()[0]

            select_end_point = """
                SELECT NEXTVAL(seq_task_unit2)
            """
            curs.execute(select_end_point)
            end_point = curs.fetchone()[0]

            prod_ids = prod_ids_all[start_point:end_point]

            prod_ids_len = len(prod_ids)

            if prod_ids_len == 0:
                drop_seq = """
                    DROP SEQUENCE seq_task_unit;
                """
                curs.execute(drop_seq)

                drop_seq2 = """
                    DROP SEQUENCE seq_task_unit2;
                """
                curs.execute(drop_seq2)
            else:
                get_shoes_review(prod_ids)
    finally:
        conn.close()

# 서울 시간 기준으로 변경
local_tz = pendulum.timezone('Asia/Seoul')

# airflow DAG설정        
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 1, tzinfo=local_tz),
    'catchup': False,
}

# DAG인스턴스 생성
dag = DAG(
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='musinsa_review_crawling_from_sql'
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
    , external_task_id='start_notify'
    , mode='reschedule'
    , dag=dag
)

# DAG 동적 생성
count = get_musinsa_count()
ids = get_category_prod_ids()

for i in range(0, count):
    review_crawling = PythonOperator(
        task_id='{0}_review_crawling'.format(i),
        python_callable=distribute_task,
        op_kwargs={'ids':ids},
        dag=dag
    )
    start_notify_sensor >> review_crawling

