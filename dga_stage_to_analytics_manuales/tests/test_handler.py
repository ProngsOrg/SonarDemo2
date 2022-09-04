import json
import unittest
import os, sys

environment = os.environ['ENVIRONMENT']

parameters_file = open(f'./dga_stage_to_analytics_manuales/tests/configs/parameters_{environment}.json')

parameters = json.load(parameters_file)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['DATABASE'] = parameters.get('database')

os.environ['LOGS_BUCKET'] = parameters.get('logs_bucket')

os.environ['ERROR_LAMBDA'] = parameters.get('error_lambda')

from dga_stage_to_analytics_manuales import writeToAnalytics,stage_to_analytics

event_file = open(f'./dga_stage_to_analytics_manuales/tests/events/event_{environment}.json')

event = json.load(event_file)

class testStageToAnalyticsManuales(unittest.TestCase):

    def setUp(self):

        self.event = event

    def testStageToAnalytics(self):

        try:

            response = stage_to_analytics(event)

            expectedResponse =  {
            'statusCode': 200,
            'body': json.dumps(f'Se ha cargado la data en formato parquet correctamente sobre la base de datos: {os.environ["DATABASE"]}')
            }

            self.assertEqual(response, expectedResponse)

        except Exception as e:

            raise Exception(str(e))