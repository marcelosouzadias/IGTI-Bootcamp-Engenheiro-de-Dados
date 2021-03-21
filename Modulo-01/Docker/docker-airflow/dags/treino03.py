# Dat Schedulada para dados do Titanic
# Primeira DAG com Airflow

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random

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
    "treino-03",
    description="Extrai dados do Titanic da Internet e calcula a idade média para homens ou mulheres",
    default_args=default_args,
    schedule_interval="*/2 * * * *"
)

# Busca o Data Set
get_data = BashOperator(
    task_id='get_data',
    bash_command = 'curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o /usr/local/airflow/data/train.csv',
    dag=dag
)

# Função que escolhe o sexo
def sorteia_h_m():
    return random.choice(['male','female'])

# Atribuição da função sorteia_h_m para o Airflow
escolhe_h_m = PythonOperator(
    task_id='escolhe-h-m',
    python_callable=sorteia_h_m,
    dag=dag
)

# Função que busca o retorno da task escolhe-h-m
def MouF(**context):
    value = context['task_instance'].xcom_pull(task_ids='escolhe-h-m')
    if value == 'male':
        return 'branch_homem'
    if value == 'female':
        return 'branch_mulher'


# Atribuição da função MouF para o Airflow
male_famele = BranchPythonOperator(
    task_id='condicional',
    python_callable=MouF,
    provide_context=True,
    dag=dag
)

# Função que realiza a media da idade dos homens
def mean_homem():
    df = pd.read_csv('/usr/local/airflow/data/train.csv')
    df_new = df.loc[df.Sex == 'male']
    print(f"Média de idade dos homens no Titanic: {df_new.Age.mean()}")


# Atribuição da funçã mean_homem paa o AirFlow
branch_homem = PythonOperator(
    task_id='branch_homem',
    python_callable=mean_homem,
    dag=dag
)

# Função que realiza a media da idade dos homens
def mean_mulher():
    df = pd.read_csv('/usr/local/airflow/data/train.csv')
    df_new = df.loc[df.Sex == 'female']
    print(f"Média de idade dos Mulheres no Titanic: {df_new.Age.mean()}")


# Atribuição da funçã mean_homem paa o AirFlow
branch_mulher = PythonOperator(
    task_id='branch_mulher',
    python_callable=mean_mulher,
    dag=dag
)

get_data >> escolhe_h_m >> male_famele >> [branch_homem, branch_mulher]