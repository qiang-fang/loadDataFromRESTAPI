# Fetch data from API and load into SQLite database
import pandas as pd
import requests
import sqlite3
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed


class LoadData:
    def __init__(self, connection):
        self.api_root = 'https://health.data.ny.gov/resource/xdss-u53e.json?'
        self.county_url = '$query=select distinct county order by county'
        self.data_url = '$query=select test_date,new_positives,cumulative_number_of_positives,' \
                        'total_number_of_tests,cumulative_number_of_tests where county='
        self.conn = connection
        self.county_list = []
        self.MAX_WORKERS = 15

    def create_all_tables(self):
        try:
            response_county = requests.get(self.api_root + self.county_url)
        except requests.exceptions.RequestException as e:
            return e
        df_county = pd.DataFrame(response_county.json())
        self.county_list = df_county['county'].tolist()
        for i in self.county_list:
            self.drop_table(i)
            self.create_table(i)

    def drop_table(self, county):
        c = self.conn.cursor()
        c.execute("drop table if exists {}".format('"ft_' + county.lower() + '"'))
        self.conn.commit()

    def create_table(self, county):
        c = self.conn.cursor()
        c.execute("""
        create table if not exists {}
        (
        test_date text,
        new_positives integer,
        cumulative_number_of_positives integer,
        total_number_of_tests integer,
        cumulative_number_of_tests integer,
        load_date text
        )
        """.format('"ft_' + county.lower() + '"'))
        self.conn.commit()

    def insert_data(self, response, county):
        today_date = date.today().strftime("%Y-%m-%d")
        cur = self.conn.cursor()
        sql = """insert into {} values(:test_date,:new_positives,:cumulative_number_of_positives,
        :total_number_of_tests,:cumulative_number_of_tests,'{}'
        )""".format('"ft_' + county.lower() + '"', today_date)
        cur.executemany(sql, response.json())
        self.conn.commit()

    def fetch_data(self, county):
        try:
            response = requests.get(self.api_root + self.data_url + '"' + county + '"')
            return response, county
        except requests.exceptions.RequestException as e:
            return e

    def runner(self):
        threads = []
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for county in self.county_list:
                threads.append(executor.submit(self.fetch_data, county))
            for task in as_completed(threads):
                print(task.result())
                self.insert_data(task.result()[0], task.result()[1])


if __name__ == '__main__':
    conn = sqlite3.connect(':memory:')
    my_load_data = LoadData(conn)
    my_load_data.create_all_tables()
    my_load_data.runner()
    conn.close()
