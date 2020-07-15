from Collector import Collector
from google.oauth2 import service_account


def collect_stock():
    # implement manually below
    cred_json_path = './google_project61024.json'

    g_cred = service_account.Credentials.from_service_account_file(cred_json_path)
    collector = Collector(save_folder="./Lake/", g_credential=g_cred)
    collector.collect_daily_data()


if __name__ == '__main__':
    collect_stock()
