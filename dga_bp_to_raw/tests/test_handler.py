import json
import unittest
import os, sys

environment = os.environ['ENVIRONMENT']

parameters_file = open(f'./dga_bp_to_raw/tests/configs/parameters_{environment}.json')

parameters = json.load(parameters_file)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['SOURCE_PATH'] = parameters.get('source_path')

os.environ['TARGET_PATH'] = 's3://raw-koandina-industrial-482883277387-dev/cl/interna/industrial/valores_bp_agua_energia/'

os.environ['TARGET_DB'] = 'db_koandina_cl_raw'

os.environ['TARGET_TABLE'] = 'bp_aguas_energia_raw'


from dga_bp_to_raw.dga_bp_to_raw import bptoraw, check_glueTable

event_file = open('./dga_bp_to_raw/tests/events/event.json')

event = json.load(event_file)

class bpToRawTest(unittest.TestCase):

    def setUp(self):

        self.event = event


    def testBPToRaw(self):
        
        try:

            response = bptoraw(event)

            expectedResponse = {
            'statusCode': 200,
            'body': json.dumps(f'Se ha cargado la data de BP correctamente en la base de datos: {os.environ["TARGET_DB"]}')
            }

            self.assertEqual(response, expectedResponse)

        except Exception as e:

            raise Exception(str(e))

    def testCheckGlueTableTrue(self):

        try:

            response = check_glueTable(os.environ['TARGET_TABLE'], os.environ['TARGET_DB'])

            expectedResponse = True

            self.assertEqual(response, expectedResponse)

        except Exception as e:

            raise Exception(str(e))

    def testCheckGlueTableFalse(self):

        try:

            response = check_glueTable('bp_aguas_energia', os.environ['TARGET_DB'])

            expectedResponse = False

            self.assertEqual(response, expectedResponse)

        except Exception as e:

            raise Exception(str(e))