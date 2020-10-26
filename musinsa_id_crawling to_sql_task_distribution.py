# crawling
import pandas as pd
import numpy
from selenium import webdriver
import re
import time
import csv
import datetime as dt
import pymysql
from sqlalchemy import create_engine
from PIL import Image
import base64
from io import BytesIO

# airflow 
from airflow import DAG
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


# 무신사 모델 정보 뽑기
def get_shoes_info(category, page):

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)

    # 모델 musinsa_id 추출 
    prod_id = []
    url = 'https://store.musinsa.com/app/items/lists/'+str(page)+'/?category=&d_cat_cd=005&u_cat_cd=&brand=&sort=pop&sub_sort=&display_cnt=3000&page=1&page_kind=category&list_kind=small&free_dlv=&ex_soldout=N&sale_goods=&exclusive_yn=&price=&color=&a_cat_cd=&size=&tag=&popup=&brand_favorite_yn=&goods_favorite_yn=&blf_yn=&campaign_yn=&bwith_yn=&price1=&price2=&chk_soldout=on'
    driver.get(url)
    time.sleep(10)
    driver.implicitly_wait(90)
    prod_id_list = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.list_img > a > img')
    for q in prod_id_list:
        raw_prod_id = q.get_attribute("data-original")
        prod_id_cook = raw_prod_id.split('/')[6]
        prod_id.append(prod_id_cook)

    # 모델 상세 정보 추출
    prod_info = []
    for prod_id_one in prod_id:
        url2 = 'https://store.musinsa.com/app/product/detail/' + str(prod_id_one) + '/0'
        driver.get(url2)
        time.sleep(1)
        driver.implicitly_wait(10)

        # 무신사 대표 이미지 가져와서 현재 디렉토리에 저장하는 코드(디렉토리 설정해주세요.)
        prod_main_img = driver.find_element_by_css_selector('#bigimg')
        img_url = prod_main_img.get_attribute('src')
        r = requests.get(img_url)
        file = open("musinsa_img_{}.jpg".format(str(prod_id_one)), "wb")
        file.write(r.content)
        file.close()

        buffer = BytesIO()
        im = Image.open("musinsa_img_{}.jpg".format(prod_id_one))

        im.save(buffer, format='jpeg')
        img_str = base64.b64encode(buffer.getvalue())

        prod_name = driver.find_element_by_class_name('product_title')
        prod_name_text = prod_name.text
        try: # 영어 이름이 있는 경우 제거
            prod_name_eng = driver.find_element_by_class_name('product_title_eng')
            prod_name_eng_text = prod_name_eng.text
            prod_name_text = prod_name_text.replace(prod_name_eng_text, '')
        except: # 영어 이름이 없는 경우 pass
            pass

        # 브랜드, id
        id_and_brand = driver.find_element_by_class_name('product_article_contents')
        prod_brand = driver.find_element_by_css_selector('#page_product_detail > div.right_area.page_detail_product > div.right_contents.section_product_summary > div.product_info > p > a:nth-child(3)')
        prod_brand_text = prod_brand.text
        prod_brand_clean = prod_brand_text.replace(' ','').replace('(','').replace(')','')
        id_and_brand_text = id_and_brand.text
        # prod_brand = id_and_brand_text.split('/')[0]  # 브랜드
        try :
            name_id = id_and_brand_text.split('/')[1]  # 모델품번
        except :
            name_id = id_and_brand_text # 품번이 없는 제품이 가끔 있음

        # 사이즈
        try:
            size = driver.find_element_by_class_name('option1')
        except: # 단일 사이즈인 제품이 아주 가끔 있어서 예외처리
            size = '사이즈 정보 없음'
        # 사이즈가 option1이 아닌 경우 예외처리
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
                    pass
            join_size_text = '-'.join(size_text)  # 사이즈.
        except:
            join_size_text = size

        # 성별
        gender = driver.find_element_by_class_name('txt_gender')
        gender_text = gender.text # 성별

        # 가격
        try:
            price = driver.find_element_by_css_selector('#goods_price > del')
        except:
            price = driver.find_element_by_css_selector('#goods_price')
        price_text = price.text # 일반가격


        # 모델 이름에서 품번, 광고성 문구, 색상 등 기타정보 제거
        modelname = ''
        if len(prod_name_text.split()) != 1: # 모델명이 품번이 아닌 경우
            if prod_name_text.startswith('['): # [로 시작하는 광고 제거, 예)[키높이]
                try:
                    if len(prod_name_text.split(']')) > 2: # []가 여러개 있는 경우
                        modelname = ''.join(prod_name_text.split(']')[2:])
                        if modelname == '': # []가 끝에 있는경우
                            modelname = ''.join(prod_name_text.split(']')[1:-1])
                    else:
                        modelname = prod_name_text.split(']')[1]
                except: # 오타 있어서 [쏠라} 와 같은 것 때매 에러남
                    modelname = prod_name_text.split('}')[1]

            elif prod_name_text.startswith('('): # (로 시작하는 추가 정보가 있는 경우 예:(비브람솔)
                modelname = ''.join(prod_name_text.split(')')[1:])
            else: # 광고성 괄호가 없는 경우
                modelname = prod_name_text

            modelname = modelname.replace(name_id,'').replace('/','') # 품번 제거
            modelname = modelname.split('(')[0].split('-')[0] # 색상, 설명 제거
        else:
            modelname = prod_name_text # 모델명이 품번인 경우

            
        prod_info.append([category, prod_brand_clean, name_id, modelname, gender_text, join_size_text, prod_id_one, price_text, img_str])

    musinsa_df = pd.DataFrame(
        data=prod_info
        , columns=['category', 'brand', 'shono', 'modelname', 'shosex', 'size', 'musinsa_id', 'price_m', 'img']
    )

    # 무신사 데이터 편집

    musinsa_df.drop(musinsa_df[musinsa_df['shosex'] == '남 여 아동'].index, axis=0, inplace=True)
    musinsa_df.drop(musinsa_df[musinsa_df['shosex'] == '아동'].index, axis=0, inplace=True)
    musinsa_df.drop(musinsa_df[musinsa_df['shosex'] == '라이프'].index, axis=0, inplace=True)
    musinsa_df.drop(musinsa_df[musinsa_df['shosex'] == '여 아동'].index, axis=0, inplace=True)

    musinsa_df['shosex'].replace('남 여', "남녀공용", inplace=True)
    musinsa_df['shosex'].replace('남', "남성용", inplace=True)
    musinsa_df['shosex'].replace('여', "여성용", inplace=True)

    musinsa_df['minsize'] = None
    musinsa_df['maxsize'] = None
    musinsa_df['sizeunit'] = None


    for i in musinsa_df.index:
        try:
            musinsa_df['minsize'][i] = int(musinsa_df['size'].str.split('-')[i][0])
            musinsa_df['maxsize'][i] = int(musinsa_df['size'].str.split('-')[i][-1])
            musinsa_df['sizeunit'][i] = \
                int(musinsa_df['size'].str.split('-')[i][1]) - int(musinsa_df['size'].str.split('-')[i][0])
        except:
            pass

    del musinsa_df['size']

    musinsa_df.to_csv(f'/root/reviews/musinsa_{category}_id.csv')


    # 마리아디비로 전송

    engine = create_engine("mysql+mysqldb://footfootbig:" + "footbigmaria!" + "@35.185.210.97/footfoot", encoding='utf-8')
    conn = engine.connect()
    try:
        musinsa_df.to_sql(name='musinsa_shoes', con=engine, if_exists='append', index=False)
    finally:
        conn.close()


# DB에서 category, page 갖고오기

def get_category_page():
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!', database='footfoot')

    try:
        with conn.cursor() as curs:
            try:
                create_seq = """
                    CREATE SEQUENCE seq_musinsa_category START WITH 1 INCREMENT BY 1;
                """
                curs.execute(create_seq)
            except:
                pass

            nextval = """
                SELECT NEXTVAL(seq_musinsa_category);
            """
            curs.execute(nextval)
            next_val = curs.fetchone()[0]

            try:
                select_brand = """
                    SELECT category, page
                      FROM musinsa_category
                     WHERE idx=%s;
                """
                curs.execute(select_brand, next_val)
                category, page = curs.fetchone()

                get_shoes_info(category, page)

            except:
                drop_seq = """
                    DROP SEQUENCE seq_musinsa_category;
                """
                curs.execute(drop_seq)

    finally:
        conn.close()


def truncate():
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:
            truncate_table = """
                truncate table musinsa_shoes;
            """
            curs.execute(truncate_table)

    finally:
        conn.close()


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
    'start_date': datetime(2020, 10, 20, tzinfo=local_tz),
    'catchup': False,
}

# DAG인스턴스 생성
dag = DAG(
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='musinsa_id_crawling_to_sql'
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
    queue='qmaria',
    dag=dag
)
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'무신사 id 크롤링이 종료되었습니다.'},
    queue='qmaria',
    dag=dag
)

# 테이블 초기화 DAG
truncate = PythonOperator(
    task_id = 'truncate',
    python_callable = truncate,
    queue = 'qmaria',
    dag = dag,
)

# DAG 동적 생성
# 크롤링 DAG
count = get_musinsa_category_count()

for count in range(0, count):
    id_crawling = PythonOperator(
        task_id='{0}_id_crawling'.format(count),
        python_callable=get_category_page,
        dag=dag
    )
    start_notify >> truncate >> id_crawling>> end_notify

