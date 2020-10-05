# crawling
import pandas as pd
import numpy
from selenium import webdriver
import re
import time
import csv

# 코드 추가
 
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
    
    prod_info = []
    
    for i in range(16):
        url = 'https://store.musinsa.com/app/items/lists/005/?category=&d_cat_cd=005&u_cat_cd=&brand=&sort=pop&sub_sort=&display_cnt=1000&page='+str(i)+'&page_kind=category&list_kind=small&free_dlv=&ex_soldout=Y&sale_goods=&exclusive_yn=&price=&color=&a_cat_cd=&size=&tag=&popup=&brand_favorite_yn=&goods_favorite_yn=&blf_yn=&campaign_yn=&bwith_yn=&price1=&price2=&chk_soldout=on'
        driver.get(url)
        #searchList

        prod_brand = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.article_info > p.item_title > a')
        prod_id_list = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.list_img > a')
        prod_name_list = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.article_info > p.list_info > a')
        for q,w,e in zip(prod_id_list,prod_name_list,prod_brand):
            raw_prod_id = q.get_attribute("href")
            prod_name = w.text
            prod_brand = e.text
            prod_id = raw_prod_id.split('/')[6]
            prod_info.append([prod_brand, prod_id, prod_name])

    filename = '/root/reviews/musinsa_prod_id.csv'
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['prod_brand', 'prod_id', 'prod_name'])
    for i in prod_info:
        csvWriter.writerow(i)
    f.close()
    driver.close()



def get_shoes_review():

    prod_id_csv = pd.read_csv('/root/reviews/musinsa_prod_id.csv')
    prod_ids = prod_id_csv['prod_id']

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    style_list = ['photo','goods']


    for style in style_list:
    
        musinsa_rvw_list = []
        img_url_list = []
        
        progress = 0
        progress_check = 0
        
        for prod_id in prod_ids:
            page_num = 0
            while True:
                page_num = page_num + 1
                url = 'https://store.musinsa.com/app/reviews/goods_estimate_list/'+str(style)+'/'+str(prod_id)+'/0/'+str(page_num)
                driver.get(url)
                prod_rvw_date = driver.find_elements_by_class_name('date')
                prod_name = driver.find_elements_by_class_name('list_info.p_name')
                prod_cust_buy_size = driver.find_elements_by_class_name('txt_option')
                prod_size_jud = driver.find_elements_by_css_selector('body > div > div > div > div.postRight > div > div.prd-level-each > ul')
                prod_rvw = driver.find_elements_by_class_name('content-review')
                prod_img_list = driver.find_elements_by_class_name('musinsa-gallery-images')
                for img_src in prod_img_list:
                    img_url = img_src.get_attribute('src')
                    img_url_list.append(img_url)
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
                        brightness = test[1]
                        color = test[2]
                        footwidth = test[3]
                        ignition = test[4]
                    except:
                        pass
                for q,w,e,r in zip(prod_rvw_date,prod_name,prod_cust_buy_size,prod_rvw):
                    musinsa_rvw_list.append([q.text, w.text, e.text, size, brightness, color, footwidth, ignition, r.text])
            n = 0
            for url in img_url_list:
                n = n + 1
                r = requests.get(url)
                file = open(f'/root/images/musinsa_{style}_{prod_id}_{n}.jpg', 'wb')
                file.write(r.content)
                file.close()
            
            # 진행상황 체크                
            progress = progress + 1
            progress_percent = round((progress * 100) / float(len(prod_ids)))
            if progress_check < progress_percent :
                progress_check = progress_percent
                progress_notify = f'무신사 {style} 리뷰 크롤링 {str(progress_percent)}% 완료되었습니다.'
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

        filename = f'/root/reviews/musinsa_reviews_{style}.csv'
        f = open(filename, 'w', encoding='utf-8', newline='')
        csvWriter = csv.writer(f)
        csvWriter.writerow(['prod_date','prod_name','prod_cust_buy_size','prod_size','prod_brightness','prod_color','prod_footwidth','prod_ignition','prod_rvw'])
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
    'catchup': False,
    'retries': 1,
    'retry_delay':timedelta(minutes=1)
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
    dag=dag
)
# id 크롤링
id_crawling_code = PythonOperator(
    task_id='id_crawling',
    python_callable=get_shoes_info,
    dag=dag
)
# 리뷰 크롤링
review_crawling_code = PythonOperator(
    task_id='review_crawling',
    python_callable=get_shoes_review,
    dag=dag
)
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'무신사 크롤링이 종료되었습니다.'},,
    dag=dag
)

# 실행 순서 설정
start_notify >> id_crawling_code >> review_crawling_code >> end_notify