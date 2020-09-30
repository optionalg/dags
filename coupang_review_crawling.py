# crawling
import pandas as pd
import numpy
from selenium import webdriver
import re
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
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

def get_shoes_review():
    # model_id 불러오기
    coupang_model_id_path = './coopang_model_id.csv'
    model_dataframe = pd.read_csv(coupang_model_id_path)
    model_ids = model_dataframe['model_id']

    coupang_reviews = []
    coupang_review_info = []
    
    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    for j in model_ids:
        page = 0
        
        while True:
            page = page + 1
           
            url = 'https://www.coupang.com/vp/product/reviews?productId=' + str(j) + '&page=' + str(page) + '&size=100&sortBy=ORDER_SCORE_ASC&ratings=&q=&viRoleCode=2&ratingSummary=true'
            driver.get(url)
            time.sleep(3)
            try:
                end_point = driver.find_element_by_css_selector('body > div.sdp-review__article__no-review.sdp-review__article__no-review--active')
                print(end_point)
                break
            except:
                pass
            time.sleep(3)
            re = driver.find_elements_by_css_selector('body > article')
            for i in re:
                review_name = i.find_elements_by_css_selector    ('body > article > div.sdp-review__article__list__info > div.sdp-review__article__list__info__user > span')
                review_day = i.find_elements_by_css_selector     ('body > article > div.sdp-review__article__list__info > div.sdp-review__article__list__info__product-info > div.sdp-review__article__list__info__product-info__reg-date')
                buy_product_info = i.find_elements_by_css_selector('body > article > div.sdp-review__article__list__info > div.sdp-review__article__list__info__product-info__name')
                review_review = i.find_elements_by_css_selector  ('body > article > div.sdp-review__article__list__review.js_reviewArticleContentContainer > div')
                my_size = i.find_elements_by_css_selector        ('body > article > div.sdp-review__article__list__survey > div:nth-child(1)')
                foot_Areview = i.find_elements_by_css_selector   ('body > article > div.sdp-review__article__list__survey > div:nth-child(2)')
                my_pdsize = i.find_elements_by_css_selector      ('body > article > div.sdp-review__article__list__survey > div:nth-child(3)')

                for rn, rd, bpi, rr in zip(review_name, review_day, buy_product_info, review_review):
                    coupang_reviews.append([rn.text, rd.text, bpi.text, rr.text])
                for rn, rd, bpi, ms, fa, mp in zip(review_name, review_day, buy_product_info, my_size, foot_Areview, my_pdsize):
                    coupang_review_info.append([rn.text, rd.text, bpi.text,ms.text, fa.text,mp.text])
                    
    driver.close()

    Refilename ='coupang_reviews.csv'
    Sifilename ='coupang_review_info.csv'

    f = open(Refilename, 'w', encoding='utf-8', newline='')
    j = open(Sifilename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriters = csv.writer(j)
    csvWriter.writerow(['이름','리뷰작성일','구매정보','리뷰'])
    for q in coupang_reviews:
        csvWriter.writerow(q)

    csvWriters.writerow(['이름','리뷰작성일','구매정보','평소싸이즈','발볼','사이즈평가'])
    for w in coupang_review_info:
        csvWriters.writerow(w)

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
      dag_id='coupang_review_crawling'
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
    op_kwargs={'context':'쿠팡 리뷰 크롤링을 시작하였습니다.'},
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
    op_kwargs={'context':'쿠팡 리뷰 크롤링이 종료되었습니다.'},
    queue='qmaria',
    dag=dag
)

# id 크롤링 종료 감지
sensor = ExternalTaskSensor(
      task_id='external_sensor'
    , external_dag_id='coupang_id_crawling'
    , external_task_id='end_notify'
    , execution_date_fn=lambda dt: dt + timedelta(minutes=1)
    , dag=dag
)

# 실행 순서 설정
sensor >> start_notify >> crawling_code >> end_notify
