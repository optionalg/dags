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
pymysql.install_as_MySQLdb()
import MySQLdb


# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import pendulum
import requests

# DB에서 b_name, page 갖고 오기

def get_b_name_page():
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:
            create_seq = """
                CREATE SEQUENCE seq_danawa_id START WITH 1 INCREMENT BY 1;
            """
            curs.execute(create_seq)

            nextval = """
                SELECT NEXTVAL(seq_danawa_id);
            """
            curs.execute(nextval)
            next_val = curs.fetchone()[0]

            try:
                select_brand = """
                    SELECT brand, page
                      FROM danawa_brand
                     WHERE idx=%s;
                """
                curs.execute(select_brand, next_val)
                b_name, page = curs.fetchone()

            except:
                drop_seq = """
                            DROP SEQUENCE seq_danawa_id;
                        """
                curs.execute(drop_seq)

    finally:
        conn.close()

    return b_name, page

# 신발 정보 가져오는 함수
def get_shoes_info(b_name, page, **kwargs):

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
        time.sleep(1)
        driver.implicitly_wait(10)
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
    danawa = pd.read_csv(f'/root/reviews/danawa_raw_{b_name}_id.csv')

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
    danawa['price'] = None

    for i in danawa.index:
        splitmo = danawa['modelname'][i].split(' ')
        for n in splitmo:
            if re.match('.*[a-zA-Z]*.*\d+.*', n):
                danawa['shono'][i] = n
                if danawa['modelname'][i][0] == 'X':
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
            if ' 출시가: ' in n:
                danawa['price'][i] = n.strip()[5:-1]

        #   카테고리 무신사기준으로 변경
        for n in range(0, len(danacate)):
            for m in range(0, len(danacate[n])):
                if danacate[n][m] in danawa['prod_info'][i]:
                    danawa['category'][i] = musincate[n]

        #   신발카테고리가 아니거나 성인용이 아닌 신발 삭제
        if (danawa['category'][i] not in musincate) or (danawa['shosex'][i] not in shosex):
            danawa.drop(i, axis=0, inplace=True)


    danawa.to_csv(f'/root/reviews/danawa_{b_name}_id.csv')

    danapath = f'/root/reviews/danawa_*_id.csv'
    dana_file_list = glob.glob(os.path.join(danapath))

    dana_df_list = []
    for file in dana_file_list:
        tmp_df = pd.read_csv(file, index_col=0, thousands=',')
        dana_df_list.append(tmp_df)

    danawa_df = pd.concat(dana_df_list, axis=0, ignore_index=True)
    del danawa_df['prod_info']

    danawa_df.rename(columns={
        'price': 'price_d'
    }
        , inplace=True
    )

    if b_name == "MLB":
        danawa_df.loc[danawa_df["brand"] == "MLB","brand"] = "엠엘비"

    danawa_df.drop_duplicates(inplace=True)


    # 마리아디비로 전송
    engine = create_engine("mysql+mysqldb://footfootbig:" + "footbigmaria!" + "@35.185.210.97/footfoot", encoding='utf-8')
    conn = engine.connect()
    try:
        danawa_df.to_sql(name='danawa_shoes', con=engine, if_exists='replace', index=False)
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
    'start_date': datetime(2020, 10, 1, tzinfo=local_tz),
    'catchup': False,
    'retries': 2,
    'retry_delay':timedelta(minutes=1)
}    
    
# DAG인스턴스 생성
dag = DAG(
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='danawa_id_crawling'
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
    op_kwargs={'context':'다나와 id 크롤링을 시작하였습니다.'},
    queue='qmaria',
    dag=dag
)
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'다나와 id 크롤링이 종료되었습니다.'},
    queue='qmaria',
    dag=dag
)
# DAG 동적 생성
# 크롤링 DAG
id_crawling = PythonOperator(
    task_id='{0}_id_crawling'.format(page),
    python_callable=get_b_name_page(),
    op_kwargs={'b_name':b_name
                ,'page':page},
    queue='q22',
    dag=dag
)
start_notify >> id_crawling >> end_notify

