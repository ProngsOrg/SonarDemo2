import json
import unittest
import os, sys

environment = os.environ['ENVIRONMENT']

parameters_file = open(f'./dga_bp_transformation/tests/configs/parameters_{environment}.json')

parameters = json.load(parameters_file)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['TARGET_BUCKET'] = parameters.get('target_bucket')
os.environ['TARGET_TABLE'] = parameters.get('target_table')
os.environ['SOURCE_TABLE'] = parameters.get('source_table')
os.environ['DATABASE'] = parameters.get('database')
os.environ['LOGS_BUCKET'] = parameters.get('logs_bucket')
os.environ['ERROR_LAMBDA'] = parameters.get('error_lambda')

from dga_bp_transformation.dga_bp_transformation import bp_transformation

event_file = open(f'./dga_bp_transformation/tests/events/event_{environment}.json')


event = json.load(event_file)

class bpTransformationTest(unittest.TestCase):

    def setUp(self):

        self.event = event

    def testBPTransformation(self):

        try:

            response = bp_transformation(self.event)

            expectedResponse = {
                'statusCode': 200,
                'body': json.dumps(f'Se ha ejecutado la transformacion de bp correctamente - en la base de datos: {os.environ["DATABASE"]}')
            }

            self.assertEqual(response, expectedResponse)

        except Exception as e:

            raise Exception(str(e))