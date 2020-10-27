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

# 신발 정보 가져오는 함수
def get_shoes_info(b_name, page):

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)

    # 크롤링한 신발들의 정보를 담을 리스트
    shoes_full_info = []

    # 리뷰 많은 순으로 정렬하여 15페이지까지만 진행
    for i in range(1,16):
        url = 'http://search.danawa.com/dsearch.php?query=%EC%8B%A0%EB%B0%9C&originalQuery=%EC%8B%A0%EB%B0%9C&previousKeyword=%EC%8B%A0%EB%B0%9C&volumeType=allvs&page='+str(i)+'&limit=120&sort=opinionDESC&list=list&boost=true&addDelivery=N&brand='+str(page)+'&tab=main'
        driver.get(url)
        time.sleep(3)
        driver.implicitly_wait(60)
        try:
            nosearchArea = driver.find_element_by_selector('#nosearchArea')
            break
        except:
            pass
        try:
            # 모델 코드, 모델 이름, 모델 정보
            prod_ids = driver.find_elements_by_class_name('relation_goods_unit')
            prod_names = driver.find_elements_by_class_name('click_log_product_standard_title_')
            prod_infos = driver.find_elements_by_class_name('spec_list')
            prod_costs = driver.find_elements_by_class_name('click_log_product_standard_price_')
            for q,w,e,r in zip(prod_ids,prod_names,prod_infos,prod_costs):
                prod_id = q.get_attribute('id')[20:]
                prod_name = w.text
                prod_info = e.text
                prod_cost = r.text
                prod_category = e.text.split(sep='/')[1]
                shoes_full_info.append([b_name, prod_id, prod_name, prod_category, prod_info, prod_cost])
        # 몇몇 브랜드에서 category를 split하지 못해 에러 발생
        except:
            prod_ids = driver.find_elements_by_class_name('relation_goods_unit')
            prod_names = driver.find_elements_by_class_name('click_log_product_standard_title_')
            prod_infos = driver.find_elements_by_class_name('spec_list')
            prod_costs = driver.find_elements_by_class_name('click_log_product_standard_price_')
            for q,w,e,r in zip(prod_ids,prod_names,prod_infos,prod_costs):
                prod_id = q.get_attribute('id')[20:]
                prod_name = w.text
                prod_info = e.text
                prod_cost = r.text
                shoes_full_info.append([b_name, prod_id, prod_name,'오류', prod_info,prod_cost])
                
    # 브랜드이름 파일명으로 저장
    filename = f'/root/reviews/danawa_raw_{b_name}_id.csv'
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['brand','danawa_id','modelname','category','prod_info','prod_cost'])
    for i in shoes_full_info:
        csvWriter.writerow(i)
    f.close()
    driver.close()

    # 저장된 파일 편
    danawa = pd.read_csv(f'/root/reviews/danawa_raw_{b_name}_id.csv', index_col=0, thousands=',')

    danawa['shono'] = None

    shosex = ['남성용', '여성용', '남녀공용']
    danawa['shosex'] = None

    danacate = [['슬립온'], ['몽크스트랩'], ['펌프스'], ['플랫'], ['샌들'], ['슬리퍼']
        , ['런닝화', '트레일런닝화', '워킹화', '마라톤화']
        , ['릿지화', '축구화', '탁구화', '운동화', '농구화', '스니커즈', '복싱화', '아쿠아트레킹화', '볼링화', '아쿠아슈즈', '트레이닝화', '테니스화', '배드민턴화',
           '인조잔디화', '포인트화', '경등산화', '중등산화', '트레킹화', '야구화']
        , ['부츠', '워커'], ['로퍼', '옥스퍼드', '컴포트화', '모카신']]
    musincate = ['캔버스', '구두', '힐', '플랫', '샌들', '슬리퍼', '러닝화', '스니커즈', '부츠', '로퍼']

    danawa['heelsize'] = None
    danawa['price_d'] = None

    for i in danawa.index:
        splitmo = danawa['modelname'][i].split(' ') # 에러
        for n in splitmo:
            if re.match('.*[a-zA-Z]*.*\d+.*', n):
                danawa['shono'][i] = n
                if danawa['modelname'][i][0] == 'X': # 에러
                    danawa['modelname'][i] = danawa['brand'][i] + ' ' + ' '.join(splitmo[1:splitmo.index(n)])
                else:
                    danawa['modelname'][i] = ' '.join(splitmo[1:splitmo.index(n)])
        #   신발 성별 추출
        for n in shosex:
            if n in danawa['prod_info'][i]:
                danawa['shosex'][i] = n
        #   굽 추출
        splitinfo = danawa['prod_info'][i].split('/')
        for n in splitinfo:
            if ' 총굽: ' in n:
                danawa['heelsize'][i] = n.strip()[3:]
        #   가격추출
        danawa['price_d'][i] = ''.join(danawa['prod_cost'][i][:-1].split(','))

        #   카테고리 무신사기준으로 변경
        for n in range(0, len(danacate)):
            for m in range(0, len(danacate[n])):
                if danacate[n][m] in danawa['prod_info'][i]:
                    danawa['category'][i] = musincate[n]

        del danawa['prod_info']

        if b_name == "MLB":
            danawa.loc[danawa["brand"] == "MLB", "brand"] = "엠엘비"
        elif b_name == 'BSQT':
            danawa.loc[danawa["brand"] == "BSQT", "brand"] = "비에스큐티"
        elif b_name == "SNRD":
            danawa.loc[danawa["brand"] == "SNRD", "brand"] = "에스엔알디"

        danawa.drop_duplicates(inplace=True)

        #   신발카테고리가 아니거나 성인용이 아닌 신발 삭제
        if (danawa['category'][i] not in musincate) or (danawa['shosex'][i] not in shosex):
            danawa.drop(i, axis=0, inplace=True)


    danawa.to_csv(f'/root/reviews/danawa_{b_name}_id.csv')


    # 마리아디비로 전송
    engine = create_engine("mysql+mysqldb://footfootbig:" + "footbigmaria!" + "@35.185.210.97/footfoot", encoding='utf-8')
    conn = engine.connect()
    try:
        danawa.to_sql(name='danawa_shoes', con=engine, if_exists='append', index=False)
    finally:
        conn.close()

def get_b_name_page():
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:

            nextval = """
                SELECT NEXTVAL(seq_danawa_brand);
            """
            curs.execute(nextval)
            next_val = curs.fetchone()[0]

            select_brand = """
                SELECT brand, page
                  FROM danawa_brand
                 WHERE idx=%s;
            """
            curs.execute(select_brand, next_val)
            b_name, page = curs.fetchone()

            get_shoes_info(b_name, page)

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
