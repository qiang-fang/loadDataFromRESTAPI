import json
import pandas as pd
import sqlite3
import unittest
from src.load_data import LoadData


class TestIntegration(unittest.TestCase):
    def test_integration(self):
        conn = sqlite3.connect(':memory:')
        test_load_data = LoadData(conn)
        test_load_data.create_all_tables()
        test_load_data.runner()
        county_list = ['Albany',
                       'Allegany',
                       'Bronx',
                       'Broome',
                       'Cattaraugus',
                       'Cayuga',
                       'Chautauqua',
                       'Chemung',
                       'Chenango',
                       'Clinton',
                       'Columbia',
                       'Cortland',
                       'Delaware',
                       'Dutchess',
                       'Erie',
                       'Essex',
                       'Franklin',
                       'Fulton',
                       'Genesee',
                       'Greene',
                       'Hamilton',
                       'Herkimer',
                       'Jefferson',
                       'Kings',
                       'Lewis',
                       'Livingston',
                       'Madison',
                       'Monroe',
                       'Montgomery',
                       'Nassau',
                       'New York',
                       'Niagara',
                       'Oneida',
                       'Onondaga',
                       'Ontario',
                       'Orange',
                       'Orleans',
                       'Oswego',
                       'Otsego',
                       'Putnam',
                       'Queens',
                       'Rensselaer',
                       'Richmond',
                       'Rockland',
                       'Saratoga',
                       'Schenectady',
                       'Schoharie',
                       'Schuyler',
                       'Seneca',
                       'Steuben',
                       'St. Lawrence',
                       'Suffolk',
                       'Sullivan',
                       'Tioga',
                       'Tompkins',
                       'Ulster',
                       'Warren',
                       'Washington',
                       'Wayne',
                       'Westchester',
                       'Wyoming',
                       'Yates']
        for county in county_list:
            with open('./fixtures/{}.json'.format(county)) as json_file:
                tmp = json.load(json_file)
                expect = pd.DataFrame(tmp, columns=['test_date', 'new_positives',
                                                    'cumulative_number_of_positives',
                                                    'total_number_of_tests',
                                                    'cumulative_number_of_tests',
                                                    'load_date'])
                actual = pd.read_sql_query("SELECT * FROM {}".format('"ft_' + county.lower() + '"'),
                                           conn)
        self.assertEqual(actual.values.tolist(), expect.values.tolist())
        conn.close()


if __name__ == '__main__':
    unittest.main()
