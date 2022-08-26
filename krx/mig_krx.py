import cx_Oracle as oci
import pymysql
from pykrx import stock
from datetime import datetime
import time


class KrxCodex:
    def __init__(self):
        self.ora_con = oci.connect('metstock/man100@192.168.60.152:1521/met12c.metel.com')
        self.my_con = pymysql.connect(host='localhost', user='metstock', password='man100', charset='utf8')
        self.dic_codes = {}
        self.code_names()
        self.stock_today = None
        self.market_types = ['KOSPI', 'KOSDAQ', 'KONEX']
        self.init_set()

    def __del__(self):
        self.ora_con.close()
        self.my_con.close()

    def init_set(self):
        _today = datetime.today().strftime('%Y%m%d')
        self.stock_today = stock.get_nearest_business_day_in_a_week(date=_today)

    def code_names(self):
        q = '''select substr(`종목코드`, 2) as code, `종목명` as name from codex.code_name'''
        cur = self.my_con.cursor()
        cur.execute(q)
        res = cur.fetchall()
        for d in res:
            self.dic_codes.update({d[0]: d[1]})

    def market_ohlcv(self, ymd):
        return stock.get_market_ohlcv(ymd, market="ALL")

    def mig_day_codex_all_m1(self, ymd):
        var_list = []
        m_df = self.market_ohlcv(ymd)
        for row in m_df.itertuples():
            code = row.Index
            if code in self.dic_codes.keys():
                open = row.시가
                high = row.고가
                low = row.저가
                close = row.종가
                volume = row.거래량
                tamt = row.거래대금
                name = self.dic_codes[code]
                var_list.append((code, ymd, name, open, high, low, close, volume, tamt))
        merge_sql = '''
                    MERGE INTO METSTOCK.CODEX_ALL_M1 d
                    USING (
                      Select
                        :1 as CODE, to_date(:2, 'YYYYMMDD') as DT, :3 as NAME, :4 as OPEN,
                        :5 as HIGH, :6 as LOW, :7 as CLOSE, :8 as VOLUME, :9 as TAMT
                      From Dual) s
                    ON
                      (d.CODE = s.CODE and 
                      d.DT = s.DT )
                    WHEN MATCHED
                    THEN
                    UPDATE SET
                      d.NAME = s.NAME,  d.OPEN = s.OPEN,  d.HIGH = s.HIGH,  d.LOW = s.LOW,
                      d.CLOSE = s.CLOSE,  d.VOLUME = s.VOLUME, d.TAMT= s.TAMT, d.UPDDT = sysdate
                    WHEN NOT MATCHED
                    THEN
                    INSERT (
                      CODE, DT, NAME,  OPEN, HIGH, LOW,  CLOSE, VOLUME, TAMT, REGDT,  UPDDT)
                    VALUES (s.CODE, s.DT, s.NAME,  s.OPEN, s.HIGH, s.LOW,  s.CLOSE, s.VOLUME, s.TAMT, sysdate,  sysdate)
                    '''
        cur = self.ora_con.cursor()
        cur.executemany(merge_sql, var_list)
        self.ora_con.commit()
        print(code, ' merge end')

    def update_codex_all_m1_today(self):
        _query = 'select max(dt) as maxday from codex_all_m1 '
        cur = self.ora_con.cursor()
        cur.execute(_query)
        res = cur.fetchone()
        _max_day = res[0].strftime('%Y%m%d')
        print(_max_day)
        _today = datetime.today().strftime('%Y%m%d')
        _workday = stock.get_nearest_business_day_in_a_week(_today)
        print(_workday)
        if _max_day != _workday:
            self.mig_day_codex_all_m1(_workday)
            print('mig_day_codex_all_m1 process done ')
        else:
            print('today is not work day')

    def update_codex_code_mt(self):
        code_db_in = []
        for mtype in self.market_types:
            _tickers = stock.get_market_ticker_list(datetime.today().strftime('%Y%m%d'), market=mtype)
            for _code in _tickers:
                _name = stock.get_market_ticker_name(_code)
                code_db_in.append((_code, _name, mtype))
        m_q = '''
                MERGE INTO METSTOCK.CODE_MT d
                USING (
                  Select
                    :1 as CODE, :2 as NAME, :3 as MARKET From Dual) s
                ON
                  (d.CODE = s.CODE )
                WHEN MATCHED
                THEN
                UPDATE SET
                  d.NAME = s.NAME, d.MARKET = s.MARKET, d.UPDDT = sysdate
                WHEN NOT MATCHED
                THEN
                INSERT (CODE, NAME, MARKET, REGDT, UPDDT)
                VALUES (s.CODE, s.NAME, s.MARKET, sysdate, sysdate)
            '''
        cur = self.ora_con.cursor()
        cur.executemany(m_q, code_db_in)
        self.ora_con.commit()
        print('merge code_mt process done')

    def select_codex_fundamental_check(self):
        query = "select code, to_char(max(dt), 'YYYYMMDD') as mxdt from codex_fundamental group by code"
        cur = self.ora_con.cursor()
        cur.execute(query)
        return cur.fetchall()

    def update_init_codex_fundamental(self):
        val_list = []
        for _code in self.dic_codes.keys():
            _df = stock.get_market_fundamental('20190104', '20220819', _code)
            for row in _df.itertuples():
                _dt = row.Index
                _bps = row.BPS
                _per = row.PER
                _pbr = row.PBR
                _eps = row.EPS
                _div = row.DIV
                _dps = row.DPS
                val_list.append((_code, _dt, _bps, _per, _pbr, _eps, _div, _dps))
            print(_code, ' market fundamental get list data ')
            time.sleep(1)

        query = '''
                MERGE INTO METSTOCK.CODEX_FUNDAMENTAL d
                USING (
                  Select
                    :1 as CODE, to_date(:2, 'YYYYMMDD') as DT, :3 as BPS, :4 as PER, :5 as PBR, :6 as EPS, :7 as DIV, :8 as DPS
                  From Dual) s
                ON
                  (d.CODE = s.CODE and d.DT = s.DT )
                WHEN MATCHED
                THEN
                UPDATE SET
                  d.BPS = s.BPS,  d.PER = s.PER,  d.PBR = s.PBR,  d.EPS = s.EPS,  d.DIV = s.DIV,  d.DPS = s.DPS, d.UPDDT = sysdate
                WHEN NOT MATCHED
                THEN
                INSERT (  CODE, DT, BPS,  PER, PBR, EPS,  DIV, DPS, REGDT,  UPDDT)
                VALUES (  s.CODE, s.DT, s.BPS,  s.PER, s.PBR, s.EPS,  s.DIV, s.DPS, sysdate,  sysdate)                
                '''
        cur = self.ora_con.cursor()
        cur.executemany(query, val_list)
        self.ora_con.commit()
        print(_code, ' fundamental process end ')


if __name__ == '__main__':
    # print(datetime.today().strftime('%Y%m%d'))
    krx = KrxCodex()
    # result = krx.select_codex_fundamental_check()
    # print(result)
    # print(krx.get_stock_today())
    # krx.update_codex_all_m1_today()
    # krx.update_codex_code_mt()
    krx.update_init_codex_fundamental()
