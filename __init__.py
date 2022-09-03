from reader import cybos_reader_codex
from krx import mig_krx
import sys
from datetime import datetime
from pykrx import stock


class Main:
    def __init__(self):
        _ymd = datetime.today().strftime('%Y%m%d')
        _workday = stock.get_nearest_business_day_in_a_week(_ymd)
        if _ymd > _workday:
            print('today is not stock working day')
            sys.exit()
        cybos_reader = cybos_reader_codex.CybosDataReaderCodex()
        if cybos_reader.connected() is True:
            cybos_reader.update_price_m1(tick_count=1000)
            cybos_reader.update_price_day(tick_count=5)
            cybos_reader.update_price_tick(tick_count=5000)
        krx_codex = mig_krx.KrxCodex()
        krx_codex.mig_day_codex_all_m1()


if __name__ == "__main__":
    Main()
