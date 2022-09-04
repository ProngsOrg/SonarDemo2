'''
fecha modificacion : 27-abr-21
autor : Carlos Montoya
decripcion : 
Transforma la informacion de indicadores BP cambiados manualmente y enviados por SFPT, para ser consumido por PBI.

Input BP manuales (sftp):
-fecha_medicion      |   string
-tag_id              |   string
-nombre              |   string
-nivel               |   string
-valor               |   string
-agua                |   string
-energia             |   string
-tipo                |   string

Output:
-fecha_medicion      |   string
-tag_id              |   string
-nombre              |   string
-nivel               |   string
-valor               |   string
-agua                |   string
-energia             |   string
-tipo                |   string
-year                |   string
-month               |   string
-day                 |   string
'''

import pandas as pd
import boto3
import awswrangler as wr
import json
import os
import logging

from pandas.core.indexes.base import Index

logger = logging.getLogger()
logger.setLevel(logging.INFO)
lmbd = boto3.client('lambda')
logs_bucket = os.environ['LOGS_BUCKET']

def bp_transformation(event):
    print(event)
    logger.info('Comenzando transformacion de los BPs')

    db = os.environ['DATABASE']
    TARGET_PATH = os.environ['TARGET_BUCKET']
    TARGET_TABLE = os.environ['TARGET_TABLE']
    SOURCE_TABLE = os.environ['SOURCE_TABLE']
    print('db',db)
    print('TARGET_PATH',TARGET_PATH)
    print('TARGET_TABLE',TARGET_TABLE)
    print('SOURCE_TABLE',SOURCE_TABLE)

    lista_archivos_bp = event.get('CheckBP').get('lista_archivos_bp')
    for item in lista_archivos_bp:
        fecha_archivo = item.split('.')[0].split('_')[0]
        year = fecha_archivo[:4]
        month = fecha_archivo[4:6]
        day = fecha_archivo[6:9]
        print(year, month, day)
        query = f''' SELECT * FROM "{SOURCE_TABLE}" WHERE year = \'{year}\' AND month = \'{month}\' AND day = \'{day}\' '''
        print(query)
        df_bp = wr.athena.read_sql_query(query, database=db, s3_output=logs_bucket)
        print(df_bp.head())
        
        try:
            
            wr.s3.to_parquet(
                df=df_bp,
                path=TARGET_PATH,  
                dataset=True,
                database=db,  
                table=TARGET_TABLE, 
                mode="overwrite_partitions",
                partition_cols=['year', 'month', 'day'])

        except Exception as e:

            raise Exception(str(e))
    
    return {
            'statusCode': 200,
            'body': json.dumps(f'Se ha ejecutado la transformacion de bp correctamente - en la base de datos: {db}')
        }
