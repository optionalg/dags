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

category_info = {
      '구두' : '005014'
    , '부츠' : '005011'
    , '힐' : '005012'
    , '플랫' : '005017'
    , '로퍼' : '005015'
    , '샌들' : '005004'
    , '슬리퍼' : '005018'
}
category_info_split = {
      '캔버스' : '018002'
    , '러닝화' : '018003'
    , '스니커즈' : '018004'
}


# 무신사 모델명, id 뽑기
def get_shoes_info(category, page, **kwargs):
    
    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    prod_info = []
    prod_id = []
    url = 'https://store.musinsa.com/app/items/lists/'+str(page)+'/?category=&d_cat_cd=005&u_cat_cd=&brand=&sort=pop&sub_sort=&display_cnt=3000&page=1&page_kind=category&list_kind=small&free_dlv=&ex_soldout=N&sale_goods=&exclusive_yn=&price=&color=&a_cat_cd=&size=&tag=&popup=&brand_favorite_yn=&goods_favorite_yn=&blf_yn=&campaign_yn=&bwith_yn=&price1=&price2=&chk_soldout=on'
    driver.get(url)
    time.sleep(60)
    prod_id_list = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.list_img > a > img')
    for q in prod_id_list:
        raw_prod_id = q.get_attribute("data-original")
        prod_id_cook = raw_prod_id.split('/')[6]
        prod_id.append(prod_id_cook)

    for prod_id_one in prod_id:
        url2 = 'https://store.musinsa.com/app/product/detail/' + str(prod_id_one) + '/0'
        driver.get(url2)
        time.sleep(3)
        # 제품코드, 브랜드 class_name 으로 찾기.
        prod_name = driver.find_element_by_class_name('product_title')
        id_and_brand = driver.find_element_by_class_name('product_article_contents')
        try:
            size = driver.find_element_by_class_name('option1')
        except:
            size = '사이즈 정보 없음'
        # 사이즈 예외처리
        try:
            size_texts = size.text
            size_text_split = size_texts.split()[2:]
            size_text = []
            # '옵션' '(3개남음)' 과 같은 이상한거 전부 제거하고 사이즈만 추출
            for regex_check in size_text_split:
                try:
                    temp = str(re.findall('2\d[0|5]',regex_check)[0])
                    size_text.append(temp)
                except:
                    size_text.append('사이즈 정보 없음')
            join_size_text = '-'.join(size_text)  # 사이즈.
        except:
            join_size_text = size
               
        gender = driver.find_element_by_class_name('txt_gender')
        try:
            price = driver.find_element_by_css_selector('#goods_price > del')
        except:
            price = driver.find_element_by_css_selector('#goods_price')
        # Name 과 brand 가 '/' 로 붙어있어서 split.
        id_and_brand_text = id_and_brand.text
        prod_brand = id_and_brand_text.split('/')[0]  # 브랜드
        try :
            name_id = id_and_brand_text.split('/')[1]  # 모델품번
        except :
            name_id = id_and_brand_text
            
        prod_name_text = prod_name.text  # 제품이름
        
        # 제품 이름에서 품번 제거
        modelname = ''
        if len(prod_name_text.split()) != 1:
            modelname = prod_name_text.replace(name_id,'').replace('/','')
        else:
            modelname = prod_name_text
        
        price_text = price.text # 일반가격

        gender_text = gender.text # 성별
            
        prod_info.append([category, prod_brand, name_id, modelname, gender_text, join_size_text, prod_id_one, price_text])

    filename = '/root/reviews/musinsa_{}_id.csv'.format(category)
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['category', 'brand', 'prod_name', 'modelname', 'gender' ,'size', 'musinsa_id','price'])
    for i in prod_info:
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
    'start_date': datetime(2020, 10, 1, tzinfo=local_tz),
    'catchup': False,
    'retries': 1,
    'retry_delay':timedelta(minutes=1)
}    
    
# DAG인스턴스 생성
dag = DAG(
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='musinsa_id_crawling'
    # DAG 설정을 넣어줌
    , default_args=default_args
    # 최대 실행 횟수
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(days=14)
)
# 크롤링 시작 알림
start_notify = PythonOperator(
    task_id='start_notify',
    python_callable=notify,
    op_kwargs={'context':'무신사 id 크롤링을 시작하였습니다.'},
    dag=dag
)
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'무신사 id 크롤링이 종료되었습니다.'},
    dag=dag
)

# DAG 동적 생성
for name, page in category_info_split.items():
    # 크롤링 DAG
    id_crawling = PythonOperator(
        task_id='{0}_id_crawling'.format(page),
        python_callable=get_shoes_info,
        op_kwargs={'category':name
                  ,'page':page},
        queue='qmaria',
        dag=dag
    )
    start_notify >> id_crawling>> >> end_notify
    
# DAG 동적 생성
for name, page in category_info.items():
    # 크롤링 DAG
    id_crawling = PythonOperator(
        task_id='{0}_id_crawling'.format(page),
        python_callable=get_shoes_info,
        op_kwargs={'category':name
                  ,'page':page},
        queue='q22',
        dag=dag
    )
    start_notify >> id_crawling>> end_notify
    