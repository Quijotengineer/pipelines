# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
import datetmime as dt

# DAG Arguments
 
# Default Arguments
default_args = {
    'owner': 'Quijotengineer',
    'start_date': today,
    'email': ['edmundodantes22ii22@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

# define the tasks

# unzip data
extract = BashOperator(
    task_id='unzip_data',
    bash_command='tar zxvf tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag,
)

# extract data from csv file
transform = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d":" -f"Rowid","Timestamp","Anonymized Vehicle number","Vehicle Type" \
        /home/project/airflow/dags/vehicle-data.csv > /home/project/airflow/dags/csv_data.csv',
    dag=dag,
)

# extract data from tsv file
load = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d":" -f"Number of axles","Tollplaza id","Tollplaza code" \
        /home/project/airflow/dags/tollplaza-data.tsv > /home/project/airflow/dags/tsv_data.csv',
    dag=dag,',
    dag=dag,
)

# extract data from fixed width file
load = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -d":" -f"Type of Payment code","Vehicle Code" \
        /home/project/airflow/dags/payment-data.txt > /home/project/airflow/dags/fixed_width_data.csv',
    dag=dag,',
    dag=dag,
)

# consolidate data extracted from previous tasks
load = BashOperator(
    task_id='consolidate_data',
    bash_command='paste /home/project/airflow/dags/csv_data.csv tsv_data.csv \
        /home/project/airflow/dags/fixed_width_data.csv > /home/project/airflow/dags/extracted_data.csv',
    dag=dag,',
    dag=dag,
)

# Transform and load the data
load = BashOperator(
    task_id='transform_data',
    bash_command='":upper" < /home/project/airflow/dags/extracted_data.csv > \
        /home/project/airflow/dags/transformed_data.csv',
    dag=dag,',
    dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width
>> consolidate_data >> transform_data
