import json
import unittest
import os, sys
import datetime

environment = os.environ['ENVIRONMENT']

parameters_file = open(f'./dga_check_bp/tests/configs/parameters_{environment}.json')

parameters = json.load(parameters_file)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['BUCKET'] = parameters.get('bucket')

os.environ['PREFIX'] = parameters.get('prefix')

os.environ['ERROR_LAMBDA'] = 'dga-notify-error_dev'

from dga_check_bp import checkBP, getLoadDate

event_file = open(f'./dga_check_bp/tests/events/event_{environment}.json')

event = json.load(event_file)

class checkBPTest(unittest.TestCase):

    def setUp(self):

        self.event = event
    

    def testGetLoadDate(self):

        try:

            response = getLoadDate(self.event)

            expected_date = datetime.datetime.strptime('2022-03-21', "%Y-%m-%d").date()
            expectedResponse = (expected_date, expected_date)

            self.assertEqual(response, expectedResponse)

        except Exception as e:

            raise Exception(str(e))

    def testCheckBP(self):

        try:

            response = checkBP(self.event)

            expectedResponse = {
                "lista_archivos_bp": [
'20220321_1517_20220101_1818_caga inicial.csv'
                ],
                "isThereData": True                
            }

            self.assertEqual(response, expectedResponse)

        except Exception as e:

            raise Exception(str(e))