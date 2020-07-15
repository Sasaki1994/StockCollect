import pandas as pd
from pandas.io import gbq
import time
import requests
from bs4 import BeautifulSoup
import re
import random
import os
import shutil
import sys
import glob
from google.oauth2 import service_account


class Collector:
    def __init__(self, save_folder, g_credential):
        if not re.match(".*/$", save_folder):
            raise Exception('require param `save_folder` be finished with `/` ')
        self.daily_data = None
        self.fetch_url = 'https://info.finance.yahoo.co.jp/ranking/'
        self.req_param = '?kd=3&tm=d&vl=a&mk=1&p={}'
        self.date = self.__get_date()
        self.save_folder = save_folder
        self.save_filename = "{}.csv".format(self.date)
        self.tmp_folder = save_folder + 'tmp/'
        self.g_cred = g_credential

    def __get_date(self):
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

    def collect_daily_data(self):
        def exit_when_already_exist(filepath):
            if os.path.exists(filepath):
                print("data has been already saved.")
                sys.exit(0)

        # judge whether already collected or not
        exit_when_already_exist(filepath=self.save_folder + self.save_filename)

        # collect(extract) stock data for ETL
        df_scrape = self.__collect_stock_data()

        # transform for ETL
        df_scrape = self.__transform_df(df_scrape)

        # save data and loading to BigQuery for ETL
        self.__save_stock_df(df_scrape)

        # remove temporary saving folder
        shutil.rmtree(self.tmp_folder)

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

        def set_tmp_or_new_df(tmp_txt, tmp_csv):
            if os.path.exists(tmp_txt):
                with open(tmp_txt) as f:
                    past_progress_page = f.read()
                    past_num = int(re.match('scraping page (\d+)/\d+', past_progress_page)[1]) + 1
                    df = pd.read_csv(tmp_csv)
            else:
                os.mkdir(self.tmp_folder)
                past_num = 1
                df = pd.DataFrame()
            return past_num, df

        tmp_txt = self.tmp_folder + '{}.txt'.format(self.date)
        tmp_csv = self.tmp_folder + '{}.csv'.format(self.date)

        # past_num:scraping page of before running
        past_num, df = set_tmp_or_new_df(tmp_txt, tmp_csv)

        # collecting start
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
            df.to_csv(tmp_csv, index=False)
            with open(tmp_txt, mode='w') as f:
                f.write(scraping_progress_str)
            # wait for server resting
            time.sleep(random.randint(30, 60))

        df.reset_index(drop=True, inplace=True)
        return df

    def __transform_df(self, df):
        df.rename(
            columns={'コード': "code", '取引値.1': "close", '出来高': "volume"},
            inplace=True)
        df = df.astype(float)
        df['code'] = df.astype(int)
        df['date'] = self.date
        return df

    def __save_stock_df(self, df):
        df.to_csv(self.save_folder + self.save_filename, index=False)
        gbq.to_gbq(df, 'stocks.sandbox_stocks', if_exists='append', credentials=self.g_cred)


if __name__ == '__main__':
    # Lake -> BigQuery program
    # implement below
    cred_json = ''

    g_cred = service_account.Credentials.from_service_account_file(cred_json)
    tmp_folder_path = './Lake/'
    stock_data_list = glob.glob(tmp_folder_path + "/*")
    df = pd.DataFrame()
    for stock_file in stock_data_list:
        date = re.match(tmp_folder_path + "(.+).csv", stock_file)[1]
        df_a = pd.read_csv(stock_file)
        df_a.rename(
            columns={"{}_close".format(date): 'close', "{}_volume".format(date): 'volume'},
            inplace=True)
        df_a['date'] = date
        df = pd.concat([df, df_a])
    # for fail safe, code below is commented out
    # gbq.to_gbq(df, 'stocks.demo_stocks', if_exists='replace', credentials=g_cred)

