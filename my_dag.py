from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
import pandas as pd
import requests

try:
    import openpyxl
except ImportError:
    import subprocess
    subprocess.run(["pip", "install", "openpyxl"])
# Define default_args dictionary to configure the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_screener_web_data(ticker_symbol):

    user = "vinaykale321@gmail.com"
    my_password = "Vinay@123#123"

    # Session to handle cookies (if needed)
    session = requests.Session()

    # Login to the website
    login_url = 'https://www.screener.in/login/'
    login_payload = {'username': user, 'password': my_password}
    session.post(login_url, data=login_payload)

    # Use the session to access the desired page
    url = f'https://www.screener.in/company/{ticker_symbol}/'
    response = session.get(url)
    

    soup = BeautifulSoup(response.content, 'html.parser')

    nos = [element.text.strip() for element in soup.find_all("span", class_="number")]
    atts = [element.text.strip() for element in soup.find_all("span", class_="name")]

    final={}
    for i in range(len(atts)):

        if i>2:
            final[atts[i]]=nos[i+1]
            #print(x[i].text,"  :",y[i+1].text )
        else:
            final[atts[i]]=nos[i]
        # print(x[i].text,"  :",y[i].text )



    print(final)
    return final

# ... (your existing code)

# Instantiate the DAG
dag = DAG(
    'my_mysql_dag',
    default_args=default_args,
    description='A simple MySQL DAG',
    schedule_interval=timedelta(days=1),
)

# Set the task dependencies, if any
# Example: load_data_task.set_upstream(other_task)
extract_screener_data = PythonOperator(
    task_id='extract_screener_data',
    python_callable=extract_screener_web_data,
    op_args=['SBIN'],
    provide_context=True,  # Ensure that the task passes the context
    dag=dag,
)

# Define a MySqlOperator task to load data into MySQL
load_data_task = MySqlOperator(
    task_id='load_data_task',
    mysql_conn_id='airflow_sql_connection',
    sql='''
    create database if not exists screener_fin_data;

    use screener_fin_data;

    create table if not exists `screener_fin_data`.`stock_data` (
        script_id int auto_increment primary key,
        script_name varchar(15) NOT null,  
        Market_Cap varchar(50) NOT null,
        Current_Price int NOT null
    );

    INSERT INTO `screener_fin_data`.`stock_data` (`script_name`, `Market_Cap`, `Current_Price`) 
    VALUES ('SBIN', '{{ ti.xcom_pull(task_ids="extract_screener_data")["Market Cap"] }}', 
                    '{{ ti.xcom_pull(task_ids="extract_screener_data")["Current Price"] }}');
    ''',
    dag=dag,
)

# Set the task dependencies
extract_screener_data >> load_data_task




