'''
fecha modificacion : 16-abr-21
autor : Roberto Gonzalez
decripcion : 
Microservicio encargado de mover la data en formato parquet entre las capas de stage y analytics haciendo uso de la tabla de dynamoDB.
Fuente de inforamcion:
-manuales agua y energia
'''
import json
import boto3
import awswrangler as wr
import pandas as pd
import logging
import pytz
import os
cht = pytz.timezone("Chile/Continental")
logger = logging.getLogger()
logger.setLevel(logging.INFO)
lmbd = boto3.client('lambda')
logs_bucket = os.environ['LOGS_BUCKET']
error_lambda = os.environ['ERROR_LAMBDA']

def writeToAnalytics(fecha, stage_table, stage_db, analytics_db, analytics_table, analytics_target):

    year = fecha[6:10]
    month = fecha[3:5]
    day = fecha[0:2]

    # Query a dga_stage
    stage_dfs = wr.athena.read_sql_query(f'SELECT * FROM "{stage_table}" WHERE year LIKE \'{year}\' AND month LIKE \'{month}\' AND day LIKE \'{day}\'', database=stage_db, chunksize=True, s3_output=logs_bucket)
    logger.info(f'SELECT * FROM "{stage_table}" WHERE year LIKE \'{year}\' AND month LIKE \'{month}\' AND day LIKE \'{day}\'')
    count = 1
    for stage_df in stage_dfs:
        if not stage_df.empty:
            try:
                wr.s3.to_parquet(
                    df=stage_df,
                    path=f"{analytics_target}",
                    dataset=True,
                    database=analytics_db,
                    table=f"{analytics_table}",
                    mode="overwrite_partitions",
                    partition_cols=['year', 'month', 'day']
                )
                logger.info(f'chunk {count} - carga realizada : {analytics_table}')
                count += 1

            except Exception as e:

                logger.info(f'Ocurrio un error - {str(e)}')

                return {
                    'statusCode': 400,
                    'body': json.dumps(f'Ocurrió un error al insertar la data en formato parquet sobre la tabla - {analytics_table} - {str(e)}')
                }

        elif stage_df.empty:
            logger.info('No se ha encontrado data para la fecha de medición indicada en la tabla: {a}'.format(a=stage_table))


def stage_to_analytics(event):
    logger.info(event)

    logger.info('Obteniendo Fecha de medicion')

    tables = event.get('Payload').get('tables')

    for data in tables:
        # se identifica que tipo de formulario es : agua o energia
        try:
            logger.info('Obteniendo fechas totales')
            lista_de_fechas_totales = event.get('Payload').get('ManualesToRaw')  
            form_type = data.split('_')[1]
            form_type_date_list = 'lista_fechas_{}'.format(form_type)
            for lista in lista_de_fechas_totales:
                logger.info(lista)
                for item in lista:
                    # revisa si form_type esta en lista
                    logger.info(item)
                    if form_type_date_list in lista:
                        lista_fechas = lista.get(form_type_date_list)
                        logger.info(f'Listado de fechas a cargar: {lista_fechas}')

            logger.info(f'Moviendo tabla {data} a capa analytics')

            table_metadata = tables.get(data)

            data_source = tables.get(data).get('s3_data_source')

            logger.info(f'loaded table metadata - {table_metadata}')
            data_source = data_source.replace('raw', 'stage') 
            stage_db = table_metadata.get('hive_database_stage')
            stage_table = table_metadata.get('hive_table_stage')
            analytics_table = table_metadata.get('hive_table_stage').replace('stage', 'analytics')
            analytics_target = table_metadata.get('s3_target').replace('stage', 'analytics')
            analytics_db = table_metadata.get('hive_database_stage').replace('stage', 'analytics')

            print('stage_db',stage_db)
            print('stage_table', stage_table)
            print('analytics_table', analytics_table)
            print('analytics_target', analytics_target)
            print('analytics_db',analytics_db)

            logger.info(f'source : source path: {data_source} | source database: {stage_db} | source table: {stage_table}')
            logger.info(f'target : target path: {analytics_target} | target database: {analytics_db} | target table: {analytics_table}')

            for fecha in lista_fechas:

                writeToAnalytics(fecha, stage_table, stage_db, analytics_db, analytics_table, analytics_target)

        except Exception as e:
            logger.info(f'No hay registros en la api para esa fecha - {str(e)}')
                      

    return {
        'statusCode': 200,
        'body': json.dumps(f'Se ha cargado la data en formato parquet correctamente sobre la base de datos: {analytics_db}')
    }
