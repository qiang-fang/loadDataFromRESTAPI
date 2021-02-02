import unittest
from src.load_data import LoadData
import sqlite3
import pandas as pd


class TestLoadData(unittest.TestCase):
    def test_create_tables(self):
        table_list = ['ft_albany',
                      'ft_allegany',
                      'ft_bronx',
                      'ft_broome',
                      'ft_cattaraugus',
                      'ft_cayuga',
                      'ft_chautauqua',
                      'ft_chemung',
                      'ft_chenango',
                      'ft_clinton',
                      'ft_columbia',
                      'ft_cortland',
                      'ft_delaware',
                      'ft_dutchess',
                      'ft_erie',
                      'ft_essex',
                      'ft_franklin',
                      'ft_fulton',
                      'ft_genesee',
                      'ft_greene',
                      'ft_hamilton',
                      'ft_herkimer',
                      'ft_jefferson',
                      'ft_kings',
                      'ft_lewis',
                      'ft_livingston',
                      'ft_madison',
                      'ft_monroe',
                      'ft_montgomery',
                      'ft_nassau',
                      'ft_new york',
                      'ft_niagara',
                      'ft_oneida',
                      'ft_onondaga',
                      'ft_ontario',
                      'ft_orange',
                      'ft_orleans',
                      'ft_oswego',
                      'ft_otsego',
                      'ft_putnam',
                      'ft_queens',
                      'ft_rensselaer',
                      'ft_richmond',
                      'ft_rockland',
                      'ft_saratoga',
                      'ft_schenectady',
                      'ft_schoharie',
                      'ft_schuyler',
                      'ft_seneca',
                      'ft_steuben',
                      'ft_st. lawrence',
                      'ft_suffolk',
                      'ft_sullivan',
                      'ft_tioga',
                      'ft_tompkins',
                      'ft_ulster',
                      'ft_warren',
                      'ft_washington',
                      'ft_wayne',
                      'ft_westchester',
                      'ft_wyoming',
                      'ft_yates']
        conn = sqlite3.connect(':memory:')
        test_object = LoadData(conn)
        test_object.create_all_tables()
        actual_list = pd.read_sql_query("select tbl_name from sqlite_master", conn)['tbl_name'].tolist()
        self.assertEqual(actual_list, table_list)
        conn.close()


if __name__ == '__main__':
    unittest.main()
