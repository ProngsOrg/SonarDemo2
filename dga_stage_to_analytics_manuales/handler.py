import json
import boto3
import os
from dga_stage_to_analytics_manuales import stage_to_analytics

error_lambda = os.environ['ERROR_LAMBDA']

def handler(event, context):

    try:

        analytics_step = stage_to_analytics(event)

        return analytics_step

    except Exception as e:

        lmbd = boto3.client('lambda')

        payload = {
            "Payload": {
                "errorMessage": str(e),
                "errorType": "Error de carga",
                "Subject": "Error Message - dga stage to analytics manuales Lambda",
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
            'body': json.dumps(f'Ocurrió un error al mover los datos de datascope entre las capas de stage y analytics - {str(e)}')
        }
