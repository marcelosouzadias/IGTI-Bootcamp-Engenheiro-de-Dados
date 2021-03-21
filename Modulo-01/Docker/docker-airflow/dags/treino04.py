from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import zipfile

# Definição de Constantes
data_path = '/usr/local/airflow/data/microdados_enade_2019/2019/3.DADOS/'
arquivo = data_path + 'microdados_enade_2019.txt'

# Argumentos Default
default_args = {
    'owner': 'Marcelo - IGTI',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 9, 20, 40),
    'email': ['marcelosouzadias@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Definição da DAG - Fluxo
dag = DAG(
    "treino-04",
    description="Paralelismo",
    default_args=default_args,
    schedule_interval="*/5 * * * *"
)

# Log de inicio do processamento
start_preprocessing = BashOperator(
    task_id='start_preprocessing',
    bash_command='echo "Start Preprocessing"',
    dag=dag
)

#Buscar os dados do Enade 2019
get_data = BashOperator(
    task_id='get_data',
    bash_command = 'curl https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o /usr/local/airflow/data/microdados_enade_2019.zip',
    dag=dag
)

# Realizar a descompcatação do arquivo zip
def unzip_file():
    with zipfile.ZipFile('/usr/local/airflow/data/microdados_enade_2019.zip', 'r') as zipped:
        zipped.extractall('/usr/local/airflow/data')


# Criação da Task no Airflow
unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_file,
    dag=dag
)

# Realiza os seguintes processos:
# - Leitura do arquivo baixado e descompactado e seleção das colunas
# - Filtragem dos campos
# - Salvar o arquivo já filtrado 
def aplicar_filtros():
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE',
            'QE_I01','QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
    print(arquivo)
    enade = pd.read_csv(arquivo, sep=';', decimal=',', usecols=cols)
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
    ]
    enade.to_csv(data_path + 'enade_filtrado.csv', index=False)

# Crição da Task no Airflow
task_aplica_filtro = PythonOperator(
    task_id='aplica_filtro',
    python_callable=aplicar_filtros,
    dag=dag
)

# Idade Centralizada na média
def constroi_idade_centralizada():
    idade = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['NU_IDADE'])
    idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()
    idade[['idadecent']].to_csv(data_path + 'idadecent.csv', index=False)

# Idade Centralizada ao quadrado
def constroi_idade_cent_quad():
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadecent['idade2'] = idadecent ** 2
    idadecent[['idade2']].to_csv(data_path + 'idadequadrado.csv', index=False)

# Criação das Task no Airflow
task_idade_cent = PythonOperator(
    task_id='constroi_idade_centralizada',
    python_callable=constroi_idade_centralizada,
    dag=dag
)

# Criação das Task no Airflow
task_idade_quad = PythonOperator(
    task_id='constroi_idade_quadrada',
    python_callable=constroi_idade_cent_quad,
    dag=dag
)


# Criação do Campo Estado Civil
def constroi_est_civil():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I01'])
    filtro['estcivil'] = filtro.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viúvo',
        'E': 'Outro'
    })
    filtro[['estcivil']].to_csv(data_path + 'estcivil.csv', index=False)

# Criação da Task no Airflow
task_est_civil = PythonOperator(
    task_id='constroi_est_civil',
    python_callable=constroi_est_civil,
    dag=dag
)

# Criação do Campo Cor da Pele
def constroi_cor():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I02'])
    filtro['cor'] = filtro.QE_I02.replace({
    'A' : 'Branca',
    'B' : 'Preta',
    'C' : 'Amarela',
    'D' : 'Parda',
    'E' : 'Indígena',
    'F' : '',
    ' ' : ''
    })
    filtro[['cor']].to_csv(data_path + 'cor.csv', index=False)

# Criação da Task no Airflow
task_cor = PythonOperator(
    task_id='constroi_cod_da_pele',
    python_callable=constroi_cor,
    dag=dag
)


# Realiza o JOIN dos dados
def join_data():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv')
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadeaoquadrado = pd.read_csv(data_path + 'idadequadrado.csv')
    estcivil = pd.read_csv(data_path + 'estcivil.csv')
    cor = pd.read_csv(data_path + 'cor.csv')

    final = pd.concat([
        filtro, idadecent, idadeaoquadrado, estcivil, cor
    ], axis=1)

    final.to_csv(data_path + 'enade_tratado.csv', index=False)
    print(final)

# Criação da Task no Airflow
task_join = PythonOperator(
    task_id='join_data',
    python_callable=join_data,
    dag=dag
)

#inicion das task
start_preprocessing >> get_data >> unzip_data >> task_aplica_filtro

# rodando em paralelo as task
task_aplica_filtro >> [task_idade_cent, task_est_civil, task_cor]

# task_idade_quad roda depois que rodar a task_idade_cent
task_idade_quad.set_upstream(task_idade_cent)

# task_join roda depois de rodar todas as task 
# task_idade_cent não e necessário pois a task_idade_quad so roda depis da task_idade_cent
task_join.set_upstream([
    task_est_civil, task_cor, task_idade_quad
])