# crawling
import pandas as pd
import numpy
from selenium import webdriver
import re
import time
import csv
import datetime as dt

# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import pendulum
import requests

# 무신사 모델명, id 뽑기
def get_shoes_info():
    
    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    model_info = []
    
    for i in range(16):
        url = 'https://store.musinsa.com/app/items/lists/005/?category=&d_cat_cd=005&u_cat_cd=&brand=&sort=pop&sub_sort=&display_cnt=1000&page='+str(i)+'&page_kind=category&list_kind=small&free_dlv=&ex_soldout=Y&sale_goods=&exclusive_yn=&price=&color=&a_cat_cd=&size=&tag=&popup=&brand_favorite_yn=&goods_favorite_yn=&blf_yn=&campaign_yn=&bwith_yn=&price1=&price2=&chk_soldout=on'
        driver.get(url)
        #searchList

        model_brand = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.article_info > p.item_title > a')
        model_id_list = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.list_img > a')
        model_name_list = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.article_info > p.list_info > a')
        for q,w,e in zip(model_id_list,model_name_list,model_brand):
            raw_model_id = q.get_attribute("href")
            model_name = w.text
            model_brand = e.text
            model_id = raw_model_id.split('/')[6]
            model_info.append([model_brand, model_id, model_name])

    filename = '/root/reviews/musinsa_model_id.csv'
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['model_brand', 'model_id', 'model_name'])
    for i in model_info:
        csvWriter.writerow(i)
    f.close()
    driver.close()



def get_shoes_review():

    model_id_csv = pd.read_csv('/root/reviews/musinsa_model_id.csv')
    model_ids = model_id_csv['model_id']

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    style_list = ['style','photo','goods']
    musinsa_rvw_list = []
    for style in style_list:
        for musinsa_model_id in model_ids:
            page_num = 0
            while True:
                try:
                    page_num = page_num + 1
                    url = 'https://store.musinsa.com/app/reviews/goods_estimate_list/'+str(style)+'/'+str(musinsa_model_id)+'/0/'+str(page_num)
                    driver.get(url)
                    model_rvw_date = driver.find_elements_by_css_selector('body > div > div > div > div.postRight > div > div.profile > p > span.date.last')
                    model_name = driver.find_elements_by_css_selector('body > div > div > div > div.postRight > div > div.connect_product.estimate-item > div.connect_review_info > div > a.list_info.p_name')
                    model_cust_buy_size = driver.find_elements_by_css_selector('body > div > div > div > div.postRight > div > div.connect_product.estimate-item > div.connect_review_info > p')
                    model_size_jud = driver.find_elements_by_css_selector('body > div > div > div > div.postRight > div > div.prd-level-each > ul')
                    model_rvw = driver.find_elements_by_css_selector('body > div > div > div > div.postRight > div > div.pContent > div.summary > div > div.pContent_text > span')
                    no_data = driver.find_element_by_class_name('mypage_review_none')
                    if no_data != None:
                        break

                except:
                    pass
                for model_size_jud_split in model_size_jud:
                    model_size_jud_text = model_size_jud_split.text
                    try:
                        test = model_size_jud_text.split('\n')
                        size = test[0]
                        brightness = test[1]
                        color = test[2]
                        footwidth = test[3]
                        ignition = test[4]
                    except:
                        pass
                for q,w,e,r in zip(model_rvw_date,model_name,model_cust_buy_size,model_rvw):
                    musinsa_rvw_list.append([q.text, w.text, e.text, size, brightness, color, footwidth, ignition, r.text])

        # 진행상황 체크                
        progress = progress + 1.0
        progress_percent = (progress * 100) / float(len(model_ids))
        progress_check = f'무신사{style} 크롤링 {progress_percent:.2f}% 완료되었습니다.'
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


    Refilename = '/root/reviews/musinsa_reviews.csv'.format(dt.strftime("%Y_%m_%d"))
    f = open(Refilename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['model_date','model_name','model_cust_buy_size','model_size','model_brightness','model_color','model_footwidth','model_ignition','model_rvw'])
    for w in musinsa_rvw_list:
        csvWriter.writerow(w)
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
      dag_id='musinsa_review_crawling'
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
    op_kwargs={'context':'무신사 크롤링을 시작하였습니다.'},
    queue='qmaria',
    dag=dag
)
# 크롤링 코드 동작
id_crawling_code = PythonOperator(
    task_id='id_crawling',
    python_callable=get_shoes_info,
    queue='qmaria',
    dag=dag
)

# 크롤링 코드 동작
review_crawling_code = PythonOperator(
    task_id='review_crawling',
    python_callable=get_shoes_review,
    queue='qmaria',
    dag=dag
)

# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'무신사 크롤링이 종료되었습니다.'},
    queue='qmaria',
    dag=dag
)

# 실행 순서 설정
start_notify >> id_crawling_code >> review_crawling_code >> end_notify