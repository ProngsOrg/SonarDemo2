import json
import boto3
from dga_bp_to_raw import bptoraw


def handler(event, context):

    try:

        data = bptoraw(event)

        return data

    except Exception as e:

        print(context)
        lmbd = boto3.client('lambda')
        
        payload = {
            "Payload": {
                "errorMessage": str(e),
                "errorType": "Error de carga",
                "Subject": "Error Message - dga bp to raw Lambda",
                "FunctionName": context.function_name
            }
        }

        response = lmbd.invoke(
            FunctionName='dga-notify-error',
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )


        print(f'Correctly executed dga-notify-error lambda - {response}')

        return {
            'statusCode': 400,
            'body': json.dumps(f'Ocurrió un error al cargar los datos de bp sobre el catalogo - {str(e)}')
        }
