# Primeira DAG com Airflow

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


# Argumentos Default
default_args = {
    'owner': 'Marcelo - IGTI',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 8, 17),
    'email': ['marcelosouzadias@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Definição da DAG - Fluxo
dag = DAG(
    "treino-01",
    description="Básico de Bash Operators e Python Operators",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

# Configuração das tarefas
hello_bash = BashOperator(
    task_id='Hello_Dash',
    bash_command='echo "Hello Airflow from Bash"',
    dag=dag
)

t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

def say_hello():
    print('Hello Airflow from Python!')

hello_python = PythonOperator(
    task_id = 'Hello_Python',
    python_callable=say_hello,
    dag=dag
)

hello_bash >> hello_python >> t1