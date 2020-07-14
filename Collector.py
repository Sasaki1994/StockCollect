import pandas as pd
import time
import requests
from bs4 import BeautifulSoup
import re
import random
import os
import shutil
import sys


class Collector:
    def __init__(self):
        self.daily_data = None
        self.fetch_url = 'https://info.finance.yahoo.co.jp/ranking/'
        self.req_param = '?kd=3&tm=d&vl=a&mk=1&p={}'
        self.date = None

    def collect_daily_data(self):
        def get_date():
            """
            data from scraping page
            :return: (str) date
            """
            req_data = requests.get(self.fetch_url + self.req_param.format(1))
            soup = BeautifulSoup(req_data.content, "html.parser")
            update_date_text = soup.find(class_="dtl yjSt").text
            date_match = re.match('最終更新日時：20(\d{2})年(\d+)月(\d+)日.*', update_date_text)
            year = date_match[1]
            month = date_match[2].zfill(2)
            day = date_match[3].zfill(2)
            self.date = '{}-{}-{}'.format(year, month, day)
            return self.date

        def exit_when_already_exist(date):
            if os.path.exists("./Lake/{}.csv".format(date)):
                print("data has been already saved.")
                sys.exit(0)

        # setting collecting data date
        get_date()

        # judge whether already collected or not
        exit_when_already_exist(self.date)

        # collect(extract) stock data for ETL
        df_scrape = self.__collect_stock_data()

        # transform for ETL
        df_scrape = self.__transform_df(df_scrape)

        # save data and loading to BigQuery for ETL
        self.__save_stock_df(df_scrape)

        # remove temporary saving folder
        shutil.rmtree('./Lake/tmp/')

        self.daily_data = df_scrape
        return df_scrape

    def __collect_stock_data(self):
        """

        :return: (DataFrame)
        """
        def get_last_page():
            """
            get index of last scraping page.
            :return: (int) last page index
            """
            req_data = requests.get(self.fetch_url + self.req_param.format(1))
            soup = BeautifulSoup(req_data.content, "html.parser")
            html_list = soup.select(".ymuiPagingBottom a")
            page_index_list = list(map(lambda html: html.text, html_list))
            try:
                max_index = int(page_index_list[-2])
                return max_index
            except Exception as e:
                print("couldn't get max page index for", e)

        tmp_file = './Lake/tmp/{}.txt'.format(self.date)
        if os.path.exists(tmp_file):
            with open(tmp_file) as f:
                past_progress_page = f.read()
                past_num = int(re.match('scraping page (\d+)/\d+', past_progress_page)[1]) + 1
                df = pd.read_csv('./Lake/tmp/{}.csv'.format(self.date))
        else:
            os.mkdir('./Lake/tmp')
            past_num = 1
            df = pd.DataFrame()

        last_page = get_last_page()
        for i in range(past_num, last_page+1):
            scraping_progress_str = 'scraping page {}/{}'.format(i, last_page)
            print(scraping_progress_str)
            scrape_url = self.fetch_url + self.req_param.format(i)
            row_df = pd.read_html(scrape_url)[0]
            row_df.drop(row_df.tail(1).index, inplace=True)
            df = pd.concat([df, row_df[['コード', '取引値.1', '出来高']]])
            del row_df
            # temporary save
            df.to_csv("./Lake/tmp/{}.csv".format(self.date), index=False)
            with open(tmp_file, mode='w') as f:
                f.write(scraping_progress_str)
            time.sleep(random.randint(30, 60))
        df.reset_index(drop=True, inplace=True)
        return df

    def __transform_df(self, df):
        df.rename(
            columns={'コード': "code", '取引値.1': "{}_close".format(self.date), '出来高': "{}_volume".format(self.date)},
            inplace=True)
        df = df.astype(float)
        df['code'] = df.astype(int)
        return df

    def __save_stock_df(self, df):
        saving_filename = "./Lake/{}.csv".format(self.date)
        df.to_csv(saving_filename, index=False)

