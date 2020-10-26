# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import sys
import pendulum

import pymysql

def id_merge_update():
    conn = pymysql.connect(host='35.185.210.97', user='footfootbig', password='footbigmaria!',
                           db='footfoot', charset='utf8')

    try:
        with conn.cursor() as curs:
            sql = """
                insert ignore into shoes(brand, shono, modelname, category, shosex, price_m
                , price_d, minsize, maxsize, sizeunit, heelsize, musinsa_id, danawa_id)
                (select m.brand, m.shono, m.modelname, m.category, m.shosex, m.price_m
                , d.price_d, m.minsize, m.maxsize, m.sizeunit, d.heelsize, m.musinsa_id, d.danawa_id
                  from musinsa_shoes as m
                left outer join danawa_shoes as d
                on m.shono=d.shono and m.brand=d.brand
                UNION
                select d.brand, d.shono, d.modelname, d.category, d.shosex, m.price_m, d.price_d
                , m.minsize, m.maxsize, m.sizeunit, d.heelsize, m.musinsa_id, d.danawa_id
                  from musinsa_shoes as m
                right outer join danawa_shoes as d
                    on m.shono=d.shono and m.brand=d.brand);
            """
            curs.execute(sql)

        conn.commit()

    finally:
        conn.close()

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
      dag_id='line_notify_id_crawling'
    # DAG 설정을 넣어줌
    , default_args=default_args
    # 최대 실행 횟수
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(minutes=5)
)

# id 크롤링 실행 감지
musinsa_id_crawling_dag_sensor = ExternalTaskSensor(
      task_id='external_sensor'
    , external_dag_id='musinsa_id_crawling'
    , external_task_id='id_update'
    , mode='reschedule'
    , dag=dag
)
danawa_id_crawling_dag_sensor = ExternalTaskSensor(
      task_id='external_sensor'
    , external_dag_id='danawa_id_crawling'
    , external_task_id='id_update'
    , mode='reschedule'
    , dag=dag
)

# ID 크롤링 종료 알림
id_end_notify = PythonOperator(
    task_id='start_notify',
    python_callable=id_merge_update,
    op_kwargs={'context':'ID 크롤링이 종료되었습니다.'},
    dag=dag
)





