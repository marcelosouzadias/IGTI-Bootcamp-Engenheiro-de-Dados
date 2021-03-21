from datetime import datetime, timedelta
import pendulum
import prefect
from prefect import task, Flow
from prefect.schedules import CronSchedule
import pandas as pd
from io import BytesIO
import zipfile
import requests
import os

import pyodbc
import sqlalchemy
import MySQLdb


retry_delay = timedelta(minutes=1)
schedule = CronSchedule(
    cron='*/10 * * * *',
    start_date=pendulum.datetime(2021,3,10,15,45, tz='America/Sao_Paulo')
)

@task
def get_raw_data():
    # Definição da Variavel com o link do Arquivo com os dados do Enade do ano de 2019
    url = 'https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip'

    # Metodo que realiza o Download via HTTP Request 
    filebytes = BytesIO(requests.get(url).content)

    myzip = zipfile.ZipFile(filebytes)
    myzip.extractall()
    path = "./microdados_enade_2019/2019/3.DADOS/"
    return path

@task
def aplica_filtros(path):
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE',
            'QE_I01','QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
    enade = pd.read_csv(path + 'microdados_enade_2019.txt', sep=';',decimal=',',usecols=cols)
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
    ]
    logger = prefect.context.get('logger')
    logger.info(enade)
    return enade

@task
def constroi_idade_centralizada(df):
    idade = df[['NU_IDADE']]
    idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()

    logger = prefect.context.get('logger')
    logger.info(idade[['idadecent']])

    return idade[['idadecent']]


@task
def constroi_idade_cent_quad(df):
    idadecent = df.copy()
    idadecent['idade2'] = idadecent.idadecent ** 2

    logger = prefect.context.get('logger')
    logger.info(idadecent[['idade2']])

    return idadecent[['idade2']]

@task
def constroi_est_civil(df):
    filtro = df[['QE_I01']]
    filtro['estcivil'] = filtro.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viúvo',
        'E': 'Outro'
    })

    logger = prefect.context.get('logger')
    logger.info(filtro[['estcivil']])

    return filtro[['estcivil']]

@task
def constroi_cor(df):
    filtro = df[['QE_I02']]
    filtro['cor'] = filtro.QE_I02.replace({
    'A' : 'Branca',
    'B' : 'Preta',
    'C' : 'Amarela',
    'D' : 'Parda',
    'E' : 'Indígena',
    'F' : '',
    ' ' : ''
    })
    
    logger = prefect.context.get('logger')
    logger.info(filtro[['cor']])
    
    return filtro[['cor']]

@task
def join_data(df, idadecent, idadequadrado, estcivil, cor):
    final = pd.concat([df, idadecent, idadequadrado, estcivil, cor],axis=1)
    final = final[['CO_GRUPO','TP_SEXO','idadecent','idade2','estcivil','cor']]
    
    logger = prefect.context.get('logger')
    logger.info(final)
    
    final.to_csv('enade_tratado.csv',index=False)

    return final

@task
def escreve_dw(df):
    engine = sqlalchemy.create_engine(
    "mysql://root:admin@localhost/enade?charset=utf8mb4"
    )
    df.to_sql("tratado",con=engine, index=False, if_exists='replace')

with Flow('Enade', schedule) as flow:
    path = get_raw_data()
    filtro = aplica_filtros(path)
    idadecent = constroi_idade_centralizada(filtro)
    idadequadrado = constroi_idade_cent_quad(idadecent)
    estcivil = constroi_est_civil(filtro)
    cor = constroi_cor(filtro)

    j = join_data(filtro, idadecent, idadequadrado, estcivil, cor)
    escreve_dw(j)

flow.register(project_name='IGTI', idempotency_key=flow.serialized_hash())
flow.run_agent()