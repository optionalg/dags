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
      'shoes' : '005014'
    , 'boots' : '005011'
    , 'hill' : '005012'
    , 'flat' : '005017'
    , 'loafer' : '005015'
    , 'boat' : '005016'
    , 'sandal' : '005004'
    , 'slipper' : '005018'
    , 'canvas' : '018002'
    , 'running' : '018003'
    , 'sneakers' : '018004'
}

# 무신사 모델명, id 뽑기
def get_shoes_info(category, page, **kwargs):
    
    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    prod_info = []
    url = 'https://store.musinsa.com/app/items/lists/'+str(page)+'/?category=&d_cat_cd=005&u_cat_cd=&brand=&sort=pop&sub_sort=&display_cnt=3000&page=1&page_kind=category&list_kind=small&free_dlv=&ex_soldout=N&sale_goods=&exclusive_yn=&price=&color=&a_cat_cd=&size=&tag=&popup=&brand_favorite_yn=&goods_favorite_yn=&blf_yn=&campaign_yn=&bwith_yn=&price1=&price2=&chk_soldout=on'
    driver.get(url)
    time.sleep(60)
    prod_brand = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.article_info > p.item_title > a')
    prod_id_list = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.list_img > a')
    prod_name_list = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.article_info > p.list_info > a')
        
    for q,w,e in zip(prod_id_list,prod_name_list,prod_brand):
        raw_prod_id = q.get_attribute("href")
        prod_name = w.text
        prod_brand = e.text
        prod_id = raw_prod_id.split('/')[6]
        
        url = f'https://store.musinsa.com/app/product/detail/{prod_id}/0'
        driver.get(url)
        time.sleep(3)
        # 제품코드, 브랜드 class_name 으로 찾기.
        id_and_brand = driver.find_element_by_class_name('product_article_contents')
        size = driver.find_element_by_class_name('option1')
        # Name 과 brand 가 '/' 로 붙어있어서 split.
        id_and_brand_text = id_and_brand.text
        prod_brand = id_and_brand_text.split('/')[0] #이거는 브랜드 필요하면 쓰세요.
        name_id = iD_and_brand_text.split('/')[1]
        # Size text 변환후 공백 제거.
        size_text = size.text
        size_text_split = size_text.split()
        # ['230','240','250','260','270'] 이렇게 리스트 형식이어서 join 으로 합치는 정규 표현식.
        join_size_text = '-'.join(size_text_split) # 이거는 사이즈 필요하면 쓰세요.

        prod_info.append([category, prod_brand, prod_id, prod_name, name_id, join_size_text])
    
    filename = '/root/reviews/musinsa_{}_id.csv'.format(category)
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['category', 'brand', 'musinsa_id', 'modelname', 'id', 'size'])
    for i in prod_info:
        csvWriter.writerow(i)
    f.close()
    driver.close()

def get_shoes_review(category, **kwargs):
    now = dt.datetime.now()
    nowDate = now.strftime('%Y_%m_%d')
    prod_id_csv = pd.read_csv('/root/reviews/musinsa_{}_id.csv.csv'.format(category))
    prod_ids = prod_id_csv['musinsa_id']

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    style_list = ['photo','goods']

    for style in style_list:
    
        musinsa_rvw_list = []
        
        #progress = 0
        #progress_check = 0
        
        for prod_id in prod_ids:
            # 이미지 크롤링 변수
            #img_url_list = []
            #img_prod_name = ''
            
            page_num = 0
            while True:
                page_num = page_num + 1
                url = 'https://store.musinsa.com/app/reviews/goods_estimate_list/'+str(style)+'/'+str(prod_id)+'/0/'+str(page_num)
                driver.get(url)
                time.sleep(3)
                prod_rvw_date = driver.find_elements_by_class_name('date')
                prod_name = driver.find_elements_by_class_name('list_info.p_name')
                prod_cust_buy_size = driver.find_elements_by_class_name('txt_option')
                prod_size_jud = driver.find_elements_by_css_selector('body > div > div > div > div.postRight > div > div.prd-level-each > ul')
                prod_rvw = driver.find_elements_by_class_name('content-review')

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
                for q,w,e,r in zip(prod_rvw_date,prod_name,prod_cust_buy_size,prod_rvw):
                    musinsa_rvw_list.append([q.text, w.text, e.text, size, footwidth, ignition, r.text])

        filename = f'/root/reviews/musinsa_{style}_{category}_reviews.csv'
        f = open(filename, 'w', encoding='utf-8', newline='')
        csvWriter = csv.writer(f)
        csvWriter.writerow(['review_date','modelname','buy_size','sizefeel','footwidthfeel','feeling','review'])
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
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'무신사 크롤링이 종료되었습니다.'},
    dag=dag
)

# DAG 동적 생성
for name, page in category_info.items():
    # 크롤링 DAG
    id_crawling = PythonOperator(
        task_id='{0}_id_crawling'.format(name),
        python_callable=get_shoes_info,
        op_kwargs={'category':name
                  ,'page':page},
        dag=dag
    )
    review_crawling = PythonOperator(
        task_id='{0}_review_crawling'.format(name),
        python_callable=get_shoes_review,
        op_kwargs={'category':name},
        dag=dag
    )
    start_notify >> id_crawling>> review_crawling >> end_notify
    
