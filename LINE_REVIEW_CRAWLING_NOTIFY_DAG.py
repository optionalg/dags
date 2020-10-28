# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import pendulum
import requests

# 입력받은 context를 라인으로 메시지 보내는 함수
def notify(context=None, xcom_push=None,**kwargs): 
    TARGET_URL = 'https://notify-api.line.me/api/notify'
    TOKEN = 'GQLQ8hOOwmtbKi9hHtS0KvLZxMhXywVWHsGeCbYGg7J'

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
    if xcom_push != None:
        kwargs['ti'].xcom_push(key=xcom_push, value=False)

def initiate(**kwargs):
    kwargs['ti'].xcom_push(key='review_crawling_start', value=True)
    kwargs['ti'].xcom_push(key='danawa_review_crawling_end', value=True)
    kwargs['ti'].xcom_push(key='musinsa_review_crawling_end', value=True)
    kwargs['ti'].xcom_push(key='naver_shopping_crawling_end', value=True)
    kwargs['ti'].xcom_push(key='naver_blog_crawling_end', value=True)

def check_review_crawling(**kwargs):
    danawa_check = True
    musinsa_check = True
    while danawa_check:
        danawa_check = kwargs['ti'].xcom_pull(key='danawa_review_crawling_end')
        if danawa_check:
            time.sleep(60*5)
    while musinsa_check:
        musinsa_check = kwargs['ti'].xcom_pull(key='musinsa_review_crawling_end')
        if musinsa_check:
            time.sleep(60*5)
            
# 서울 시간 기준으로 변경
local_tz = pendulum.timezone('Asia/Seoul')
today = datetime.today()
# airflow DAG설정        
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(today.year, today.month, today.day, tzinfo=local_tz) - timedelta(hours=25),
    'catchup': False,
    'provide_context': True
}    
# DAG인스턴스 생성
dag = DAG(
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='line_notify_review_crawling'
    # DAG 설정을 넣어줌
    , default_args=default_args
    # 최대 실행 횟수
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(days=1)
)
# 초기화
initiate = PythonOperator(
    task_id='initiate',
    python_callable=initiate,
    dag=dag
)
# 리뷰 크롤링 시작 알림
review_start_notify = PythonOperator(
    task_id='review_start_notify',
    python_callable=notify,
    op_kwargs={'context':'리뷰 크롤링을 시작하였습니다.'
              ,'xcom_push':'review_crawling_start'},
    dag=dag
)

check_review_crawling = PythonOperator(
    task_id='check_review_crawling',
    python_callable=check_review_crawling,
    dag=dag
)
# 리뷰 크롤링 종료 알림
review_end_notify = PythonOperator(
    task_id='review_end_notify',
    python_callable=notify,
    op_kwargs={'context':'리뷰 크롤링이 종료되었습니다.'},
    dag=dag
)

# 처리 순서
initiate >> review_start_notify >> check_review_crawling >> review_end_notify
