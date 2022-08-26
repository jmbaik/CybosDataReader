from reader import cybos_reader_codex
from krx import mig_krx


class Main:
    def __init__(self):
        cybos_reader = cybos_reader_codex.CybosDataReaderCodex()
        if cybos_reader.connected() is True:
            cybos_reader.update_price_m1(tick_count=1000)
            cybos_reader.update_price_day(tick_count=5)
            cybos_reader.update_price_tick(tick_count=5000)
        krx_codex = mig_krx.KrxCodex()
        krx_codex.mig_day_codex_all_m1()


if __name__ == "__main__":
    Main()
