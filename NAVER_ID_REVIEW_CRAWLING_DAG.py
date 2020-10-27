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
import glob, os
from sqlalchemy import create_engine

# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import sys
import pendulum
import requests

def get_naver_brand_count():
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:
            select_count = """
                SELECT count(*) from naver_brand;
            """
            curs.execute(select_count)
            count = curs.fetchone()[0]

    finally:
        conn.close()

    return count

# 신발 정보 가져오는 함수
def get_shoes_review(b_name, page):

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    naver_info_and_rvw = []
    for page in range(15):
        url = 'https://search.shopping.naver.com/search/all?brand='+str(b_name)+'&origQuery=%EC%8B%A0%EB%B0%9C&pagingIndex=' + str(page) + '&pagingSize=80&productSet=model&query=%EC%8B%A0%EB%B0%9C&sort=review&timestamp=&viewType=list'
        driver.get(url)
        time.sleep(3)
        url_list = []
        prod_url_list = driver.find_elements_by_css_selector(
            '#__next > div > div.container > div.style_inner__18zZX > div.style_content_wrap__1PzEo > div.style_content__2T20F > ul > div > div > li > div > div.basicList_img_area__a3NRA > div > a')
        for prod_url_attr in prod_url_list:
            base_url = prod_url_attr.get_attribute('href')
            url_list.append(base_url)
        for url in url_list:
            driver.get(url)  # get = 이동시키는 역할
            time.sleep(2)
            driver.implicitly_wait(10)
            prod_name = driver.find_element_by_css_selector(
                '#container > div.summary_area > div.summary_info._itemSection > div > div.h_area > h2')
            prod_name_text = prod_name.text
            brand = driver.find_element_by_css_selector(
                '#container > div.summary_area > div.summary_info._itemSection > div > div.goods_info > div > span:nth-child(2) > em')
            brand_text = brand.text

            # 네이버 대표 이미지 가져와서 현재 디렉토리에 저장하는 코드(디렉토리 설정해주세요.)
            #prod_main_img = driver.find_element_by_css_selector('#viewImage')
            #img_url = prod_main_img.get_attribute('src')
            #r = requests.get(img_url)
            #file = open("naver_img_{}.jpg".format(str(prod_name_text)), "wb")
            #file.write(r.content)
            #file.close()

            all_review_counts = driver.find_element_by_css_selector('#snb > ul > li.mall_review > a > em')
            end_page = int(all_review_counts.text) / 20
            try:
                for page in range(1, int(end_page)):
                    driver.execute_script(f"shop.detail.ReviewHandler.page({page+1}, '_review_paging'); return false;")
                    time.sleep(5)
                    prod_infos = driver.find_elements_by_css_selector(
                        '#_review_list > li > div > div.avg_area > span > span:nth-child(4)')
                    review_dates = driver.find_elements_by_css_selector(
                        '#_review_list > li > div > div.avg_area > span > span:nth-child(3)')
                    reviews = driver.find_elements_by_css_selector('#_review_list > li > div > div.atc')
                    for review_date, prod_info, review in zip(review_dates, prod_infos, reviews):
                        review_date_text = review_date.text
                        prod_info_text = prod_info.text
                        review_text = review.text
                        naver_info_and_rvw.append([brand_text, prod_name_text, review_date_text, prod_info_text, review_text])
            except:
                pass
    refilename = f'/root/reviews/naver_{b_name}.csv'
    f = open(refilename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['brand', 'prod_name', 'review_info','review_date', 'reviews'])
    for w in naver_info_and_rvw:
        csvWriter.writerow(w)
    f.close()

def get_b_name_page():
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:

            nextval = """
                SELECT NEXTVAL(seq_naver_brand);
            """
            curs.execute(nextval)
            next_val = curs.fetchone()[0]

            select_brand = """
                SELECT brand, page
                  FROM naver_brand
                 WHERE idx=%s;
            """
            curs.execute(select_brand, next_val)
            b_name, page = curs.fetchone()

            get_shoes_review(b_name, page)

    finally:
        conn.close()


def truncate():
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')
    try:
        with conn.cursor() as curs:
            truncate_table = """
                truncate table danawa_shoes;
            """
            curs.execute(truncate_table)
            try:
                drop_seq = """
                    DROP SEQUENCE seq_danawa_brand;
                """
                curs.execute(drop_seq)
            except:
                pass
            create_seq = """
                CREATE SEQUENCE seq_danawa_brand START WITH 1 INCREMENT BY 1;
            """
            curs.execute(create_seq)
    finally:
        conn.close()
        
def drop_seq():
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!', database='footfoot')

    try:
        with conn.cursor() as curs:
            drop_seq = """
                DROP SEQUENCE seq_danawa_brand;
            """
            curs.execute(drop_seq)
    except:
        pass
    finally:
        conn.close()
        
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
      dag_id='danawa_id_crawling_to_sql'
    # DAG 설정을 넣어줌
    , default_args=default_args
    # 최대 실행 횟수
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(minutes=5)
)

# 시작 감지
start_notify_sensor = ExternalTaskSensor(
      task_id='external_sensor'
    , external_dag_id='line_notify_id_crawling'
    , external_task_id='id_start_notify'
    , mode='reschedule'
    , dag=dag
)

# 테이블 초기화 DAG
truncate = PythonOperator(
    task_id='truncate',
    python_callable=truncate,
    dag=dag,
)

# 테이블 초기화 DAG
drop_seq = PythonOperator(
    task_id = 'drop_seq',
    python_callable = drop_seq,
    dag = dag,
)

# DAG 동적 생성
# 크롤링 DAG
count = get_danawa_brand_count()

for count in range(0, count):
    id_crawling = PythonOperator(
        task_id='{0}_id_crawling'.format(count),
        python_callable=get_b_name_page,
        dag=dag
    )
    start_notify_sensor >> truncate >> id_crawling >> drop_seq