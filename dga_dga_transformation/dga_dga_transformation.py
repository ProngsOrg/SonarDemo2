'''
fecha modificacion  : 03-may-21
ultima modificacion : Carlos Montoya
decripcion: 
Transforma la informacion de Agua proveniente de Siemens (pozos) para su utilizacion en PBI.
Fuente de inforamcion:
-dga_analitycs

Input:
-codigo_ob       |   string
-fecha_medicion  |   string
-hora_medicion   |   string
-caudal          |   string        | caudal en m3/h. 
-totalizador     |   string        | m3 de agua.
-nivel_freatico  |   string        | nivel freatico en metros.
-year            |   string
-month           |   string
-day             |   string
Output:
-codigo_ob       |  string         | Proveniente de la fuente. 
-fecha_medicion  |  string         | Proveniente de la fuente.
-hora_medicion   |  string         | Proveniente de la fuente.
-caudal          |  string         | caudal en m3/h.  
-totalizador     |  datetime64[ns] | m3 de agua.
-nivel_freatico  |  string         | nivel freatico en metros.
-year            |  string         | particion.
-month           |  string         | particion.
-day             |  string         | particion.
-turno           |  string         | Se asocia el turno a la hora del registro. 
-fecha_hora      |  string         | Fecha y hora en la misma columna. Ayuda al sort.
-diff_totalizado |  float64        | Calculo de las diferencias de los totalizados por indicador.
'''

import json
import boto3
import awswrangler as wr
import os
import numpy as np
import time
import pandas as pd
import pytz
import datetime
from datetime import datetime, date, timedelta
import warnings
import logging

warnings.filterwarnings("ignore")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
lmbd = boto3.client('lambda')
logs_bucket = os.environ['LOGS_BUCKET']
list_codigo_ob = os.environ['LIST_CODIGO_OB']
list_codigo_ob = list_codigo_ob.strip('][').split(',')
logger.info(f'Lista de codigos OB a procesar: {list_codigo_ob}')
error_lambda = os.environ['ERROR_LAMBDA']

def getLoadDate(event):
    
    if event.get('Payload').get('fecha_inicio') != "":

        fecha_medicion = event.get('Payload').get('fecha_inicio')

        fecha_anterior = datetime.strptime(fecha_medicion, "%d-%m-%Y").date() - timedelta(days=1)

        fecha_anterior = fecha_anterior.strftime("%d-%m-%Y")

    return fecha_medicion, fecha_anterior

def iterateTurnos(df, df_turno_list):

    for i in df_turno_list:
        if i < 7:
            df.loc[df['hora'] == i, ['turno']] = '1_8001'
        if i >= 22:
            df.loc[df['hora'] == i, ['turno']] = '4_8001'
        if i >= 7 and i < 15:
            df.loc[df['hora'] == i, ['turno']] = '2_8002'
        if i >= 15 and i < 22:
            df.loc[df['hora'] == i, ['turno']] = '3_8003'


    return df

def iterateCodigoOB(df, list_codigo_ob):
    # se crea dataframe vacio para agregar cada indicador.
    df_dga = pd.DataFrame()

    # Se itera atraves del listado de codigos para obtener el diferenciado para cada registro
    for codigo in list_codigo_ob:
        temp_df = df.loc[df['codigo_ob'] == codigo]
        temp_df = temp_df.astype({'totalizador': 'float64'})
        temp_df['diff_totalizado'] = temp_df['totalizador'].diff()
        df_dga = pd.concat([df_dga, temp_df])
    print('iteratecodigoob')
    print(df_dga)
    return df_dga

def dga_transformation(event):
    logger.info('Comenzando transformacion de la tabla DGA')
    logger.info(event)
    
    db = os.environ['DATABASE']
    target_table = os.environ['TARGET_TABLE']
    target = os.environ['TARGET_BUCKET']
    
    logger.info(f'Target Glue DB: {db} | Target Glue table: {target_table} | Target path: {target}')
    
    fecha_medicion, fecha_anterior = getLoadDate(event)

    year = fecha_medicion[6:10]
    month = fecha_medicion[3:5]
    day = fecha_medicion[:2]

    year_yst = fecha_anterior[6:10]
    month_yst = fecha_anterior[3:5]
    dia_busqueda_yst = fecha_anterior[0:2]
    
    logger.info('Fecha de procesamiento {}-{}-{}'.format(day, month, year))
    
    try:
        logger.info('Obteniendo registros de la tabla - dga_analytics')

        # query retorna informacion del dia actual y el dia anterior
        query = ''' SELECT * FROM "dga_analytics"  WHERE year = '{}' and month = '{}' and day = '{}' '''.format(year, month, day)
        logger.info(f'Query: {query} | {db}')
        print(query)
        filtered_results = wr.athena.read_sql_query(query, database=db, ctas_approach=False,  s3_output=logs_bucket)
        logger.info(f'dataframe head: {filtered_results.head()}')
        logger.info(f'dataframe len: {len(filtered_results)}')
        print('filtered_results')
        print(filtered_results.head())
        query_yst = ''' SELECT * FROM "dga_analytics"  WHERE year = '{}' and month = '{}' and  day = '{}' '''.format(year_yst, month_yst, dia_busqueda_yst)
        logger.info(f'{query_yst} - {db}')
        print('results_yesterday')
        results_yesterday = wr.athena.read_sql_query(query_yst, database=db,  s3_output=logs_bucket)
        logger.info(f'dataframe head ayer: {results_yesterday.head()}')
        logger.info(f'dataframe len ayer: {len(results_yesterday)}')
        print(results_yesterday.head())
        if results_yesterday.empty:
            logger.info('yesterday results are empty')
            
            results_yesterday = None
            
        else:
            logger.info('yesterday results are not empty')
            print(results_yesterday.head())
            filtered_results = pd.concat([filtered_results, results_yesterday])

        logger.info(f'dataframe head concat: {filtered_results.head()}')
        logger.info(f'dataframe len concat: {len(filtered_results)}')

        logger.info('Iniciando procesamiento de la informacion')
        
        filtered_results = filtered_results.dropna()
        logger.info(f'dataframe len drop: {len(filtered_results)}')
        filtered_results['nivel_freatico'] = filtered_results['nivel_freatico'].replace('#N/D', '0')
        filtered_results['caudal'] = filtered_results['caudal'].replace('#N/D', '0')
        print('filtered_results_2')
        print(filtered_results.head())
        df = pd.DataFrame()
        for codigo in list_codigo_ob:
            temp_df = filtered_results[filtered_results['codigo_ob'] == codigo]
            logger.info(f'Codigo OB: {codigo} | len: {len(temp_df)}')
            df = pd.concat([df, temp_df])
        print('after codigo ob')
        print(df.head())
        logger.info(f'Tamano datafame filtrado por codigos ob a procesar: {len(df)}')
        
        try:
            df['nivel_freatico'] = df['nivel_freatico'].str.replace(',', '.')
            df['caudal'] = df['caudal'].str.replace(',', '.')

        except Exception as e:
            logger.info(f'No fue posible parsear el nivel freatico ni el caudal - {str(e)}')

        df = df.astype({
            'nivel_freatico': 'float64',
            'totalizador': 'float64',
            'caudal': 'float64'
        })

        df['hora'] = pd.to_datetime(df['hora_medicion']).dt.hour
        df['minutos'] = pd.to_datetime(df['hora_medicion']).dt.minute
        df_turno_list = df.pivot_table(index=['hora'], aggfunc='size').index
        df['turno'] = ""

        df = iterateTurnos(df, df_turno_list)

        df.loc[(df['hora'] == 14) & (df['minutos'] > 29), ['turno']] = '3_8003'
        df.drop(columns=['hora', 'minutos'], inplace=True)
        df = df.astype({'turno': 'string'})
        df['fecha_hora'] = pd.to_datetime(df['fecha_medicion'] + ' ' + df['hora_medicion'], dayfirst=True)
        df = df.sort_values(by='fecha_hora')

        df_dga = iterateCodigoOB(df, list_codigo_ob)
        print(df_dga.head())
        df_dga = df_dga[df_dga['day'] == day]
        logger.info(f'dataframe head final: {df_dga.head()}')
        logger.info(f'dataframe len final: {len(df_dga)}')
        df_dga['fecha_medicion'] = pd.to_datetime(df_dga['fecha_medicion'])
        
        if not df_dga.empty:
            
            try:
                logger.info('Iniciando escritura')
                wr.s3.to_parquet(
                    df=df_dga,
                    path=target,
                    dataset=True,
                    database=db,
                    table=target_table,
                    mode="overwrite_partitions",
                    partition_cols=['year', 'month', 'day'])
        
                logger.info(f'Se ha cargado la tabla correctamente - {target_table}')
        
                return {
                    'statusCode': 200,
                    'body': json.dumps(f'Se cargo correctamente la tabla - {target_table}')
                }
        
            except Exception as e:
        
                logger.info(f'Ocurrio un error - {str(e)}')
        
                payload = {
                    "Payload": {
                        "errorMessage": str(e),
                        "errorType": "Error de transformacion",
                        "Subject": "Error Message - DGA Transformation lambda"
                    }
                }
                
                logger.info(payload)
                
                response = lmbd.invoke(
                    FunctionName=error_lambda,
                    InvocationType='RequestResponse',
                    Payload=json.dumps(payload)
                )
                logger.info(response)

                print(f'Correctly executed dga-notify-error lambda - {response}')

                return {
                    'statusCode': 400,
                    'body': json.dumps(f'Ocurrió un error al insertar la data en formato parquet sobre la tabla - {target_table} - {str(e)}')
                }
        
        else:
            
            return {
                'statusCode': 200,
                'body': json.dumps(f'No existen datos para la fecha a cargar para la tabla - {target_table}')
            }

    except Exception as e:

        payload = {
            "Payload": {
                "errorMessage": str(e),
                "errorType": "Error al obtener datos",
                "Subject": "Error Message - DGA Transformation lambda"
            }
        }

        response = lmbd.invoke(
            FunctionName=error_lambda,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )

        print(f'Correctly executed dga-notify-error lambda - {response}')

        logger.info(
            f'Ocurrio un error al obtener los datos de la tabla dga_analytics - {str(e)}')

        return {
            'statusCode': 400,
            'body': f'Ocurrio un error al obtener los datos de la tabla dga_analytics - {str(e)}'
        }

