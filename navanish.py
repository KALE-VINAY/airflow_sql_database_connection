from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.decorators import apply_defaults

import requests
from bs4 import BeautifulSoup
import pandas as pd

def extract_data(**kwargs):
    ticker_symbol = 'SBIN'
    url = f'https://www.screener.in/company/{ticker_symbol}/'
    res = requests.get(url)
    return res.text

def transform_data(**context):
    html_text = context['task_instance'].xcom_pull(task_ids='extract_data')
    soup = BeautifulSoup(html_text, 'html.parser')
    nos = [element.text.strip() for element in soup.find_all("span", class_="number")]
    atts = [element.text.strip() for element in soup.find_all("span", class_="name")]
    final={}
    for i in range(len(atts)):
        if i>2:
            final[atts[i]]=nos[i+1]
        else:
            final[atts[i]]=nos[i]
    return final

def load_data(**context):
    final = context['task_instance'].xcom_pull(task_ids='transform_data')
    df = pd.DataFrame(list(final.items()), columns=['att', 'nos'])
    df.insert(0, 'uid', range(1, 1 + len(df)))
    data_tuples = list(df.itertuples(index=False, name=None))
    return data_tuples

class MySqlInsertOperator(BaseOperator):
    @apply_defaults
    def _init_(self, mysql_conn_id, sql, xcom_task_id=None, *args, **kwargs):
        super(MySqlInsertOperator, self)._init_(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.sql = sql
        self.xcom_task_id = xcom_task_id

    def execute(self, context):
        data_tuples = context['task_instance'].xcom_pull(task_ids=self.xcom_task_id)
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        for params in data_tuples:
            mysql_hook.run(self.sql, parameters=params)

default_args = {
    'owner': 'navanish',
    'start_date': datetime(2024, 1, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL DAG for web scraping',
    schedule_interval=timedelta(days=1),
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

insert_task = MySqlInsertOperator(
    task_id='insert_data_task',
    mysql_conn_id='aflw',
    sql="INSERT INTO scrapy_afw.web_scrap (uid, att, nos) VALUES (%s, %s, %s)",
    xcom_task_id='load_data',
    dag=dag
)

extract_task >> transform_task >> load_task >> insert_task