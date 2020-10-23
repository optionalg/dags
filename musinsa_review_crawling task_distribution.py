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

def get_musinsa_category_count():
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:
            select_count = """
                SELECT count(*) from musinsa_category;
            """
            curs.execute(select_count)
            count = curs.fetchone()[0]

    finally:
        conn.close()

    return count

def get_shoes_review(category, prod_ids):
    now = dt.datetime.now()
    prod_id_csv = pd.read_csv('/root/reviews/musinsa_{}_id.csv'.format(category))
    prod_ids = prod_id_csv['musinsa_id']

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)

    style_list = ['photo','goods']

    for style in style_list:

        musinsa_rvw_list = []

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
                #모델이름
                try:
                    no_data = driver.find_element_by_class_name('mypage_review_none')
                    if no_data != None:
                        break

                except:
                    pass
                for prod_size_jud_split in prod_size_jud:
                    prod_size_jud_text = prod_size_jud_split.text
                    try:
                        test = prod_size_jud_text.split('\n')
                        size = test[0]
                        footwidth = test[3]
                        ignition = test[4]
                    except:
                        pass
                for q,e,r in zip(prod_rvw_date,prod_cust_buy_size,prod_rvw):
                    #musinsa_rvw_list.append([q.text, prod_id, e.text, size, footwidth, ignition, r.text])
                    filename = f'/home/reviews/reviews.txt'
                    f = open(filename, 'w', encoding='utf-8', newline='')
                    review_date = q.text
                    review = r.text
                    f.write(f'{prod_id} {review_date} {review}\n')
                    f.close()
        
    driver.close()


def get_category_prod_ids(prod_ids_len=None, category=None, ids=None):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    if prod_ids_len == None or prod_ids_len == 0:
        try:
            with conn.cursor() as curs:
                try:
                    create_seq = """
                        CREATE SEQUENCE seq_musinsa_id START WITH 1 INCREMENT BY 1;
                    """
                    curs.execute(create_seq)
                except:
                    pass

                nextval = """
                    SELECT NEXTVAL(seq_musinsa_id);
                """
                curs.execute(nextval)
                next_val = curs.fetchone()[0]

                try:
                    select_category = """
                        SELECT category
                          FROM musinsa_category
                         WHERE idx = %s
                         ;
                    """
                    curs.execute(select_category, next_val)
                    category = curs.fetchone()[0]

                except:
                    drop_seq = """
                                DROP SEQUENCE seq_musinsa_id;
                            """
                    curs.execute(drop_seq)

                select_musinsa_id = """
                    SELECT musinsa_id
                      FROM musinsa_shoes
                     WHERE category = %s
                     ;
                """
                curs.execute(select_musinsa_id, category)
                ids = curs.fetchall()

                distribute_task(category, ids)
    else:
        distribute_task(category, ids)


def distribute_task(category, ids):
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
                get_shoes_review(category, prod_ids)
    finally:
        conn.close()

    return prod_ids_len, category, ids

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
    'start_date': datetime(2020, 10, 1, tzinfo=local_tz),
    'catchup': False,
    'retries': 2,
    'retry_delay':timedelta(minutes=1)
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
    , schedule_interval=timedelta(minutes=1)
)
# 크롤링 시작 알림
start_notify = PythonOperator(
    task_id='start_notify',
    python_callable=notify,
    op_kwargs={'context':'무신사 리뷰 크롤링을 시작하였습니다.'},
    dag=dag
)
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'무신사 리뷰 크롤링이 종료되었습니다.'},
    dag=dag
)


# DAG 동적 생성
count = get_musinsa_category_count()
prod_ids_len = None
category = None
ids = None

for i in range(0, count):
    review_crawling = PythonOperator(
        task_id='{0}_review_crawling'.format(count),
        python_callable=get_category_prod_ids,
        op_kwargs={prod_ids_len:prod_ids_len,
                   category:category,
                   ids:ids},
        dag=dag
    )

    start_notify >> review_crawling >> end_notify

