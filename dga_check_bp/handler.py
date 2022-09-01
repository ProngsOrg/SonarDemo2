import json
import boto3
import os
from dga_check_bp import checkBP

error_lambda = os.environ['ERROR_LAMBDA']

def handler(event, context):

    try:

        data = checkBP(event)

        return data

    except Exception as e:

        lmbd = boto3.client('lambda')

        payload = {
            "Payload": {
                "errorMessage": str(e),
                "errorType": "Error de carga",
                "Subject": "Error Message - dga check bp Lambda",
                "FunctionName": context.function_name
            }
        }

        response = lmbd.invoke(
            FunctionName=error_lambda,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        print(f'Correctly executed dga-notify-error lambda - {response}')

        return {
            'statusCode': 400,
            'body': json.dumps(f'Ocurrió un error al validar los archivos de bp - {str(e)}')
        }
