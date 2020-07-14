from Collector import Collector


def collect_stock():
    collector = Collector()
    collector.collect_daily_data()


if __name__ == '__main__':
    collect_stock()
