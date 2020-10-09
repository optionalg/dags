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

# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import sys
import pendulum
import requests

brand_info = {
    '골든구스' : '10697', '반스' : '10720', '라코스테' : '6559', '돔바' : '13645', '컨버스' : '10986'
    , '프레드페리' : '10601', '메종마르지엘라' : '35048', '버버리' : '10562', '락포트' : '10821', '알렉산더맥퀸' : '14288'
    , '탠디' : '10812', '엘칸토' : '10859', '리차드' : '6642', '발렌시아가' : '10803', '소다' : '6953'
    , '발렌티노' : '10741', 'MLB' : '10579', '오니츠카타이거' : '10388857', '구찌' : '10794', '닥스' : '44805'
    , '제옥스' : '10913' , '엑셀시오르' : '27451', '프리웨이' : '6641', '아떼바네사브루노' : '42907' , '스타지오네바이엘칸토' : '36625'
    , '폴스미스' : '10462', '생로랑파리' : '10756', '크록스' : '10828', '슈펜' : '30033', '미소페' : '10698'
    , '프라다' : '10561' , '지방시' : '10735', '핏플랍' : '10867', '영에이지' : '14582092', '플로쥬' : '29793216'
    , '아디다스' : '10851', '나이키' : '13876', '뉴발란스' : '13760', '리복' : '13770', '휠라' : '10789'
    , '푸마' : '12042', '프로스펙스' : '25922412', '아식스' : '6345', '디스커버리익스페디션' : '29957', '르까프' : '27161'
    , '스케쳐스' : '13949', '미즈노' : '31561', '월드컵' : '26402', '노스페이스' : '29956', '브룩스' : '10405600'
    , '요넥스' : '13806', '르꼬끄' : '5248', '슬레진저' : '18865', '호카오네오네' : '25462089', '언더아머' : '31563'
    , '카파' : '13997', '데상트' : '11764', '맥스' : '34861', '케이스위스' : '11028', '네파' : '13755'
    , '가데옴므' : '35419', '포트폴리오' : '14582122', '와키앤타키' : '6680', '리갈' : '6339', '에스콰이아' : '14582020'
    , '토즈' : '10891', '제니아에센셜' : '20642094', '레노마' : '10537', '제니아' : '20642088', '보닌' : '7061'
    , '레페토' : '10866', '엘리자벳' : '10822', '페라가모' : '10090', '잭앤질슈즈' : '11193', '가이거' : '11101'
    , '심플리트' : '14582323', '세라' : '10831', '고세' : '10824', '스퍼' : '6363', '포멜카멜레' : '35961'
    , '바바라' : '6364', '메쎄' : '10912', '레이첼콕스' : '10840' , '베카치노' : '9519', '토리버치' : '10773'
    , '버켄스탁' : '10935', '페이퍼플레인' : '35422', '슈스파' : '10430', '테바' : '14156', 'SNRD' : '35423'
    , '닥터마틴' : '10747', '팀버랜드' : '10942', '무다' : '35421', '알도' : '13911', '쏘로굿' : '10749'
    , '호킨스' : '10719' , '수페르가' : '10750', '스티유' : '13922', '라그라치아' : '11681188', '아멜리에' : '27810'
}

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
        time.sleep(3)
        try:
            nosearchArea = driver.find_element_by_selector('#nosearchArea')
            break
        except:
            pass
        try:
            # 모델 코드, 모델 이름, 모델 정보
            prod_ids = driver.find_elements_by_class_name('relation_goods_unit')
            prod_names = driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div[3]/div[2]/div[7]/div[2]/div[2]/div[3]/ul/li/div/div[2]/p/a')
            prod_infos = driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div[3]/div[2]/div[7]/div[2]/div[2]/div[3]/ul/li/div/div[2]/dl/dd/div')
            for q,w,e in zip(prod_ids,prod_names,prod_infos):
                prod_id = q.get_attribute('id')[20:]
                prod_name = w.text
                prod_info = e.text
                prod_category = e.text.split(sep='/')[1]
                shoes_full_info.append([b_name, prod_id, prod_name, prod_category, prod_info])
        # 몇몇 브랜드에서 category를 split하지 못해 에러 발생
        except:
            prod_ids = driver.find_elements_by_class_name('relation_goods_unit')
            prod_names = driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div[3]/div[2]/div[7]/div[2]/div[2]/div[3]/ul/li/div/div[2]/p/a')
            prod_infos = driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div[3]/div[2]/div[7]/div[2]/div[2]/div[3]/ul/li/div/div[2]/dl/dd/div')
            for q,w,e in zip(prod_ids,prod_names,prod_infos):
                prod_id = q.get_attribute('id')[20:]
                prod_name = w.text
                prod_info = e.text
                shoes_full_info.append([b_name, prod_id, prod_name,'오류', prod_info])
                
    # 브랜드이름 파일명으로 저장
    filename = f'/root/reviews/danawa_{b_name}_id.csv'
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['brand','danawa_id','modelname','category','prod_info'])
    for i in shoes_full_info:
        csvWriter.writerow(i)
    f.close()
    driver.close()

    # 저장된 파일 편
    danawa = pd.read_csv('/root/review/danawa_{b_name}_id.csv')

    splitmo = danawa['modelname'].str.split(' ')
    danawa['shono'] = ''

    shosex = ['남성용', '여성용', '남녀공용']
    danawa['shosex'] = ''

    danacate = [['슬립온'], ['컴포트화'], ['펌프스'], ['플랫'], ['샌들'], ['슬리퍼']
        , ['런닝화', '트레일런닝화', '워킹화', '마라톤화'], ['운동화', '농구화', '스니커즈', '복싱화', '아쿠아트레킹화']
        , ['부츠', '워커'], ['로퍼,옥스퍼드']]
    musincate = ['캔버스/단화', '구두', '힐', '플랫', '샌들', '슬리퍼', '러닝화', '스니커즈', '부츠', '로퍼']

    danawa['heelsize'] = ''
    danawa['price'] = ''

    splitinfo = danawa['prod_info'].str.split('/')

    for i in danawa.index:
        #   품번 추출
        danawa['shono'][i] = splitmo[i][-1]
        #   모델명 추출
        danawa['modelname'][i] = ' '.join(splitmo[i][1:-1])
        #   신발 성별 추출
        for n in range(0, len(shosex)):
            if shosex[n] in danawa['prod_info'][i]:
                danawa['shosex'][i] = shosex[n]
        if danawa['shosex'][i] == '':
            danawa.drop(i, axis=0, inplace=True)

    danawa.reset_index(drop=True, inplace=True)
        # 카테고리 무신사 기준으로 수정
    for i in danawa.index:
        for n in range(0, len(danacate)):
            for m in range(0, len(danacate[n])):
                if danacate[n][m] in danawa['prod_info'][i]:
                    danawa['category'][i] = musincate[n]
        if danawa['category'][i] not in musincate:
            danawa.drop(i, axis=0, inplace=True)

    danawa.reset_index(drop=True, inplace=True)

        #   굽, 가격 추출
    for i in danawa.index:
        for n in range(0, len(splitinfo)):
            if ' 총굽: ' in splitinfo[n]:
                danawa['heelsize'][i] = splitinfo[n].strip()[3:]
            if ' 출시가: ' in splitinfo[n]:
                danawa['price'][i] = splitinfo[n].strip()[5:-1]

    danawa.to_csv('/root/reviews/danawa_{b_name}_id.csv', encoding='utf-8')
    
def get_shoes_review(b_name, **kwargs):

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)

    # prod_id 불러오기
    danawa_prod_id_path = '/root/reviews/danawa_{}_id.csv'.format(b_name)
    prod_dataframe = pd.read_csv(danawa_prod_id_path)
    prod_ids = prod_dataframe['danawa_id']

    danawa_reviews = []

    #progress = 0
    #progress_check = 0

    for prod_id in prod_ids:
        #img_url_list = []
        page = 0
        while True:
            page = page + 1
            url = 'http://prod.danawa.com/info/dpg/ajax/companyProductReview.ajax.php?t=0.10499996477784657&prodCode='+str(prod_id)+'&cate1Code=1824&page='+str(page)+'&limit=100&score=0&sortType=&usefullScore=Y&innerKeyword=&subjectWord=0&subjectWordString=&subjectSimilarWordString=&_=1600608005961'
            driver.get(url)
            time.sleep(3)
            rvw_date = driver.find_elements_by_xpath('/html/body/div/div[3]/div[2]/ul/li/div[1]/span[2]')
            rvw_list = driver.find_elements_by_xpath('/html/body/div/div[3]/div[2]/ul/li/div[2]/div[1]/div[2]')

            try:
                no_data = driver.find_element_by_class_name('no_data')
                if no_data != None:
                    break
            except:
                pass
            for q,w in zip(rvw_date,rvw_list):
                danawa_reviews.append([prod_id,q.text,w.text])

    filename ='/root/reviews/danawa_{}_reviews.csv'.format(b_name)
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['danawa_id','review_date','reviews'])
    for i in danawa_reviews:
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
    'start_date': datetime(2020, 9, 30, tzinfo=local_tz),
    'catchup': False,
    'retries': 1,
    'retry_delay':timedelta(minutes=1)
}    
    
# DAG인스턴스 생성
dag = DAG(
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='danawa_review_crawling'
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
    op_kwargs={'context':'다나와 크롤링을 시작하였습니다.'},
    dag=dag
)
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'다나와 크롤링이 종료되었습니다.'},
    dag=dag
)
# DAG 동적 생성
for b_name, page in brand_info.items():
    # 크롤링 DAG
    id_crawling = PythonOperator(
        task_id='{0}_id_crawling'.format(page),
        python_callable=get_shoes_info,
        op_kwargs={'b_name':b_name
                    ,'page':page},
        dag=dag
    )
    review_crawling = PythonOperator(
        task_id='{0}_review_crawling'.format(page),
        python_callable=get_shoes_review,
        op_kwargs={'b_name':b_name},
        dag=dag
    )
    start_notify >> id_crawling>> review_crawling >> end_notify
    
