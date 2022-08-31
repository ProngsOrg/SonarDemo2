import boto3
import datetime
from datetime import timedelta, date
import pytz
import logging
import os
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lmbd = boto3.client('lambda')
bucket = os.environ['BUCKET']
prefix = os.environ['PREFIX']
cht = pytz.timezone("Chile/Continental")
fecha = datetime.datetime.today()
error_lambda = os.environ['ERROR_LAMBDA']

def getLoadDate(event):

    date_format_string = "%d-%m-%Y"
    fecha_inicio = event.get('Payload').get('fecha_inicio', '')
    fecha_fin = event.get('Payload').get('fecha_fin', '')
    print(fecha_inicio)
    print(fecha_fin)

    if fecha_inicio == "":
        today = datetime.datetime.now()
        fecha_inicio = today.astimezone(cht) - timedelta(days=1)
        fecha_inicio = fecha_inicio.strftime(date_format_string)
        fecha_inicio_busqueda = datetime.datetime.strptime(fecha_inicio, date_format_string).date()
        fecha_fin_busqueda = datetime.datetime.strptime(fecha_inicio, date_format_string).date()
        # print(year, month, day)
        logger.info(f'Procesamiento diario : {fecha_inicio}')

    elif fecha_inicio != "" and fecha_fin == "":
        fecha_inicio_busqueda = datetime.datetime.strptime(fecha_inicio, date_format_string).date()
        fecha_fin_busqueda = datetime.datetime.strptime(fecha_inicio, date_format_string).date()
        logger.info(f'Procesando fecha : {fecha_inicio}')

    elif fecha_inicio != "" and fecha_fin != "":
        fecha_inicio_busqueda = datetime.datetime.strptime(fecha_inicio, date_format_string).date()
        fecha_fin_busqueda = datetime.datetime.strptime(fecha_fin, date_format_string).date()
        logger.info(f'Procesando fechas : {fecha_inicio} - {fecha_fin}')

    return fecha_inicio_busqueda, fecha_fin_busqueda

def checkBP(event):
    s3 = boto3.client('s3')
    logger.info(f'Payload: {event}')
    fecha_inicio_busqueda, fecha_fin_busqueda = getLoadDate(event)
    print(fecha_inicio_busqueda)
    print(fecha_fin_busqueda)
    # crea lista con todas las fechas del rango
    lista_rango_fechas = []
    while fecha_inicio_busqueda <= fecha_fin_busqueda:
        fecha_medicion = fecha_inicio_busqueda.strftime("%d-%m-%Y")
        lista_rango_fechas.append(fecha_medicion)
        fecha_inicio_busqueda = fecha_inicio_busqueda + timedelta(days=1)
    #print('lista_rango_fechas: ',lista_rango_fechas)
    
    # Lista todos los elementos del path. Si encuentra un archivo .csv
    # evalua si la fecha en el nombre del archivo 20210423_1157_bp_aguas_energia.csv
    # (23-04-2021) se encuentra dentro del rango de fechas.
    lista_elementos = s3.list_objects_v2(Bucket=bucket, Prefix=f'{prefix}').get('Contents')
    lista_archivos_bp = []
    for item in lista_elementos:
        print(item)
        try:
            if item.get('Key').split('/')[-1].split('.')[1] == 'csv':
                nombre_archivo = item.get('Key').split('/')[-1]
                print(nombre_archivo)
                fecha_archivo = nombre_archivo.split('.')[0].split('_')[0]
                year_temp = fecha_archivo[:4]
                month_temp = fecha_archivo[4:6]
                day_temp = fecha_archivo[6:9]
                fecha_archivo = day_temp+'-'+month_temp+'-'+year_temp
                if fecha_archivo in lista_rango_fechas:
                    lista_archivos_bp.append(nombre_archivo)
                    logger.info(f'{nombre_archivo} esta dentro del rango con fecha: {fecha_archivo}')
                else:
                    logger.info(f'{nombre_archivo} no esta dentro del rango con fecha: {fecha_archivo}')
        except Exception as e:
            
            logger.info(f'No se han encontrado archivos bp -{str(e)}')

    
    print(lista_archivos_bp)
    if lista_archivos_bp:
        logger.info(f'Se encontro data para los bp en la fechas indicada')
        return {
            'lista_archivos_bp': lista_archivos_bp,
            'isThereData': True
        }
    else:
        logger.info(f'No se encontro data para los bp en la fechas indicada')
        return {
            'isThereData': False
        }

