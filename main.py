from Collector import Collector


def collect_stock():
    collector = Collector(save_folder="./Lake/")
    collector.collect_daily_data()


if __name__ == '__main__':
    collect_stock()
