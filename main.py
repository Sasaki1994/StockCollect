from Collector import Collector
import datetime as dt
import jpholiday as jp


def collect_stock():
    collector = Collector()
    try:
        collector.collect_daily_data()
    except:
        try:
            collector.collect_daily_data()
        except:
            collector.collect_daily_data()


if __name__ == '__main__':
    collect_stock()
