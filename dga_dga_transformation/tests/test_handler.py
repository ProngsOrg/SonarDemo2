import json
import unittest
import os, sys

environment = os.environ['ENVIRONMENT']

parameters_file = open(f'./dga_dga_transformation/tests/configs/parameters_{environment}.json')

parameters = json.load(parameters_file)

os.environ['DATABASE'] = parameters.get('database')
os.environ['TARGET_TABLE'] = parameters.get('target_table')
os.environ['TARGET_BUCKET'] = parameters.get('target_bucket')
os.environ['LIST_CODIGO_OB'] = parameters.get('list_codigo_ob')
os.environ['LOGS_BUCKET'] = parameters.get('logs_bucket')
os.environ['ERROR_LAMBDA'] = parameters.get('error_lambda')

from dga_dga_transformation import dga_transformation, getLoadDate, iterateCodigoOB, iterateTurnos

event_file = open(f'./dga_dga_transformation/tests/events/event_{environment}.json')

event = json.load(event_file)


class dgaTransformationTest(unittest.TestCase):

    def setUp(self):

        self.event = event
    
    def testDgaTransformation(self):

        try:

            response = dga_transformation(self.event)

            expected_response = {
                    'statusCode': 200,
                    'body': json.dumps(f'Se cargo correctamente la tabla - {os.environ["TARGET_TABLE"]}')
            }

            self.assertEqual(response, expected_response)
        except Exception as e:

            raise Exception(f'failure{str(e)}')