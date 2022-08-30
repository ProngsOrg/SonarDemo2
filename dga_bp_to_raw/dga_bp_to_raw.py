import boto3
import datetime
from datetime import timedelta, date
import pytz
import logging
import os
import json
import awswrangler as wr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lmbd = boto3.client('lambda')
SOURCE_PATH = os.environ['SOURCE_PATH']
TARGET_PATH = os.environ['TARGET_PATH']
TARGET_DB = os.environ['TARGET_DB']
TARGET_TABLE = os.environ['TARGET_TABLE']

def check_glueTable(TARGET_TABLE,TARGET_DB):
    glue_client = boto3.client('glue')
    print('validando tabla glue bp')
    try:
        glue_client.get_table(DatabaseName=TARGET_DB, Name=TARGET_TABLE)
        return True
    except Exception as e:
        print('La tabla {} no existe en la base de datos {} - {}'.format(TARGET_TABLE,TARGET_DB, str(e)))
        return False

def bptoraw(event):
    lista_archivos_bp = event.get('Payload').get('CheckBP').get('lista_archivos_bp')
    print(lista_archivos_bp)
    for item in lista_archivos_bp:
        fecha_archivo = item.split('.')[0].split('_')[0]
        year_temp = fecha_archivo[:4]
        month_temp = fecha_archivo[4:6]
        day_temp = fecha_archivo[6:9]
        print(year_temp, month_temp, day_temp)

        if check_glueTable(TARGET_TABLE, TARGET_DB) is False:
            print('Creando tabla {} en db {}.'.format(TARGET_TABLE, TARGET_DB))
            wr.catalog.create_csv_table(
                database=TARGET_DB, 
                table=TARGET_TABLE,
                path = f"{SOURCE_PATH}'year={year_temp}/month={month_temp}/day={day_temp}/",    
                columns_types={
                    'fecha_medicion': 'string',        
                    'tag_id': 'string',
                    'nombre': 'string',        
                    'nivel': 'string',
                    'valor': 'string',        
                    'agua': 'string',
                    'energia': 'string',
                    'tipo': 'string'
                },
                partitions_types = {'year' : 'string',
                                    'month'  : 'string',
                                    'day' : 'string'},
                skip_header_line_count=1,
                sep=','
            )
    try:

        wr.catalog.add_csv_partitions(
            database = TARGET_DB,
            table = TARGET_TABLE,
            partitions_values ={
                f"{SOURCE_PATH}'year={year_temp}/month={month_temp}/day={day_temp}/" : [year_temp, month_temp, day_temp]
            }
        )

        print('Se han cargado las particiones de bp correctamente sobre el catalogo')

        return {
                'statusCode': 200,
                'body': json.dumps(f'Se ha cargado la data de BP correctamente en la base de datos: {TARGET_DB}')
            }

    except Exception as e:

        return {
            'statusCode': 400,
            'body': json.dumps(f'Ocurrió un error al cargar los datos de bp sobre el catalogo - {str(e)}')
        }