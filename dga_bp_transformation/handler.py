import json
import boto3
import os
from dga_bp_transformation import bp_transformation

error_lambda = os.environ['ERROR_LAMBDA']

def handler(event, context):

    try:

        transformation = bp_transformation(event)

        return transformation

    except Exception as e:
        
        lmbd = boto3.client('lambda')

        payload = {
            "Payload": {
                "errorMessage": str(e),
                "errorType": "Error de carga",
                "Subject": "Error Message - dga bp transformation Lambda",
                "FunctionName": context.function_name
            }
        }

        response = lmbd.invoke(
            FunctionName='dga-notify-error_dev',
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )

        print(f'Correctly executed dga-notify-error lambda - {response}')

        return {
            'statusCode': 400,
            'body': json.dumps(f'Ocurrió un error al ejecutar la transformacion de bp - {str(e)}')
        }
