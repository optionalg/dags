# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import sys
import pendulum
import requests

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
    'start_date': datetime(2020, 11, 1, tzinfo=local_tz),
    'catchup': False,
}    

#===================================================#
#===================id_crawling=====================#
#===================2 주마다 실행========================#

# DAG인스턴스 생성
dag = DAG(
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='line_notify_id_crawling'
    # DAG 설정을 넣어줌
    , default_args=default_args
    # 최대 실행 횟수
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(days=14)
)
# ID 크롤링 시작 알림
id_start_notify = PythonOperator(
    task_id='id_start_notify',
    python_callable=notify,
    op_kwargs={'context':'ID 크롤링을 시작하였습니다.'},
    dag=dag
)

# merge 실행 감지
id_update_merge_dag_sensor = ExternalTaskSensor(
      task_id='external_sensor'
    , external_dag_id='id_merge_update'
    , external_task_id='id_merge_update'
    #, mode='reschedule'
    , dag=dag
)
# ID 크롤링 종료 알림
id_end_notify = PythonOperator(
    task_id='id_end_notify',
    python_callable=notify,
    op_kwargs={'context':'ID 크롤링이 종료되었습니다.'},
    dag=dag
)

# 처리 순서
id_start_notify >> id_update_merge_dag_sensor >> id_end_notify

#===================================================#
#=================review_crawling===================#
#===================매 일 실행==========================#

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
# 리뷰 크롤링 시작 알림
review_start_notify = PythonOperator(
    task_id='review_start_notify',
    python_callable=notify,
    op_kwargs={'context':'리뷰 크롤링을 시작하였습니다.'},
    dag=dag
)

# 무신사 실행 감지
musinsa_review_crawling_sensor = ExternalTaskSensor(
      task_id='external_sensor'
    , external_dag_id='musinsa_review_crawling'
    , external_task_id='review_analyze'
    #, mode='reschedule'
    , dag=dag
)
# 다나와 실행 감지
danawa_review_crawling_sensor = ExternalTaskSensor(
      task_id='external_sensor'
    , external_dag_id='danawa_review_crawling'
    , external_task_id='review_analyze'
    #, mode='reschedule'
    , dag=dag
)
# 리뷰 크롤링 종료 알림
review_end_notify = PythonOperator(
    task_id='review_end_notify',
    python_callable=notify,
    op_kwargs={'context':'리뷰 크롤링이 종료되었습니다.'},
    dag=dag
)

# 처리 순서
review_start_notify >> [musinsa_review_crawling_sensor, danawa_review_crawling_sensor] >> review_end_notify
