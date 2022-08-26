# coding=utf-8
import gc
import string

import pandas as pd
import pymysql
import tqdm
import sqlalchemy

from api import cybosApi
from utils import is_market_open, available_latest_date, preformat_cjk


class CybosDataReaderCli:
    def __init__(self):
        sqlalchemy_database_uri = 'mysql+mysqlconnector://metstock:man100@localhost:3306/codex'
        self.conn = pymysql.connect(host='localhost', user='metstock', password='man100', db='codex', charset='utf8')
        self.sql_engine = sqlalchemy.create_engine(sqlalchemy_database_uri, echo=False, encoding='utf-8')
        self.objStockChart = cybosApi.CybosChart()
        self.objCodeMgr = cybosApi.CybosCodeMgr()
        self.rcv_data = dict()  # RQ후 받아온 데이터 저장 멤버

        self.sv_code_df = pd.DataFrame()
        self.db_code_df = pd.DataFrame()

        sv_code_list = self.get_code_list()
        sv_name_list = list(map(self.objCodeMgr.get_code_name, sv_code_list))
        self.sv_code_df = pd.DataFrame({'종목코드': sv_code_list, '종목명': sv_name_list}, columns=('종목코드', '종목명'))

    def __del__(self):
        self.conn.close()

    def get_code_list(self):
        return self.objCodeMgr.get_code_list(1) + self.objCodeMgr.get_code_list(2)

    def code_name_db(self):
        self.sv_code_df.to_sql('code_name', self.sql_engine, if_exists='replace')

    def update_price_db(self, tick_unit='day', tick_count=None, ohlcv_only=False):
        """
        db_path: db 파일 경로.
        tick_unit: '1min', '5min', 'day'. 이미 db_path가 존재할 경우, 입력값 무시하고 기존에 사용된 값 사용.
        ohlcv_only: ohlcv 이외의 데이터도 저장할지 여부. 이미 db_path가 존재할 경우, 입력값 무시하고 기존에 사용된 값 사용
                    'day' 아닌경우 False 선택 불가 고정.
        """
        if tick_unit != 'day':
            ohlcv_only = True
        len_table = 7
        if tick_unit == '1min':
            len_table = 8
        # 로컬 DB에 저장된 종목 정보 가져와서 dataframe으로 저장
        sss = "SELECT TABLE_NAME FROM information_schema.TABLES where TABLE_SCHEMA='codex' and TABLE_NAME not in ('company_info', 'daily_price')" \
              " and length(table_name) = {}".format(len_table)
        sdf = pd.read_sql(sss, self.conn)
        db_code_list = sdf['TABLE_NAME'].values.tolist()
        db_name_list = list(map(self.objCodeMgr.get_code_name, db_code_list))

        db_latest_list = []
        for db_code in db_code_list:
            sql = "SELECT date FROM {} ORDER BY date DESC LIMIT 1".format(db_code)
            sdf = pd.read_sql(sql, self.conn)
            db_latest_list.append(sdf['date'].values[0])

        # 현재 db에 저장된 'date' column의 tick_unit 확인
        # 현재 db에 저장된 column 명 확인. (ohlcv_only 여부 확인)
        if db_latest_list:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT date FROM {} ORDER BY date ASC LIMIT 2".format(db_code_list[0]))
                date0, date1 = cursor.fetchall()
                # 날짜가 분 단위 인 경우
                if date0[0] > 99999999:
                    if date1[0] - date0[0] == 5:  # 5분 간격인 경우
                        tick_unit = '5min'
                    else:  # 1분 간격인 경우
                        tick_unit = '1min'
                elif date0[0] % 100 == 0:  # 월봉인 경우
                    tick_unit = 'month'
                elif date0[0] % 10 == 0:  # 주봉인 경우
                    tick_unit = 'week'
                else:  # 일봉인 경우
                    tick_unit = 'day'
                # column개수로 ohlcv_only 여부 확인
                q1 = "select count(0) as cnt from information_schema.COLUMNS where TABLE_SCHEMA='codex' and TABLE_NAME='{}'".format(db_code_list[0])
                cursor.execute(q1)
                row = cursor.fetchone()
                if int(row[0]) > 6:  # date, o, h, l, c, v
                    ohlcv_only = False
                else:
                    ohlcv_only = True

        db_code_df = pd.DataFrame({'종목코드': db_code_list, '종목명': db_name_list, '갱신날짜': db_latest_list},
                                  columns=('종목코드', '종목명', '갱신날짜'))
        fetch_code_df = self.sv_code_df

        # 분봉/일봉에 대해서만 아래 코드가 효과가 있음.
        if not is_market_open():
            latest_date = available_latest_date()
            if tick_unit == 'day':
                latest_date = latest_date // 10000
            # 이미 DB 데이터가 최신인 종목들은 가져올 목록에서 제외한다
            already_up_to_date_codes = db_code_df.loc[db_code_df['갱신날짜'] == latest_date]['종목코드'].values
            fetch_code_df = fetch_code_df.loc[fetch_code_df['종목코드'].apply(lambda x: x not in already_up_to_date_codes)]

        if tick_count is None:
            if tick_unit == '1min':
                count = 200000  # 서버 데이터 최대 reach 약 18.5만 이므로 (18/02/25 기준)
            elif tick_unit == '5min':
                count = 100000
            elif tick_unit == 'day':
                count = 10000  # 10000개면 현재부터 1980년 까지의 데이터에 해당함. 충분.
            elif tick_unit == 'week':
                count = 2000
            elif tick_unit == 'month':
                count = 500
        else:
            count = tick_count

        if tick_unit == '1min':
            tick_range = 1
        elif tick_unit == '5min':
            tick_range = 5
        elif tick_unit == 'day':
            tick_range = 1

        if ohlcv_only:
            columns = ['open', 'high', 'low', 'close', 'volume']
        else:
            columns = ['open', 'high', 'low', 'close', 'volume',
                       'shares', 'foreign_order_limit', 'foreign_cur_qty', 'foreign_cur_rate', 'org_net_buying', 'org_net_acc']
        '''
            상장주식수 : shares
            외국인한도수량 : foreign_order_limit
            외국인현보유수량 : foreign_cur_qty
            외국인현보유비율 : foreign_cur_rate
            기관순매수 : org_net_buying
            기관누적순매수 : org_net_acc
        '''
        with self.conn.cursor() as cursor:
            tqdm_range = tqdm.trange(len(fetch_code_df), ncols=100)
            for i in tqdm_range:
                code = fetch_code_df.iloc[i]
                update_status_msg = '[{}] {}'.format(code[0], code[1])
                tqdm_range.set_description(preformat_cjk(update_status_msg, 25))

                from_date = 0
                if code[0] in db_code_df['종목코드'].tolist():
                    cursor.execute("SELECT date FROM {} ORDER BY date DESC LIMIT 1".format(code[0]))
                    from_date = cursor.fetchall()[0][0]

                if tick_unit == 'day':  # 일봉 데이터 받기
                    if not self.objStockChart.req_chart_dwm(code[0], ord('D'), count, self, from_date, ohlcv_only):
                        continue
                elif tick_unit == '1min' or tick_unit == '5min':  # 분봉 데이터 받기
                    if not self.objStockChart.req_chart_mt(code[0], ord('m'), tick_range, count, self, from_date, ohlcv_only):
                        continue
                elif tick_unit == 'week':  # 주봉 데이터 받기
                    if not self.objStockChart.req_chart_dwm(code[0], ord('W'), count, self, from_date, ohlcv_only):
                        continue
                elif tick_unit == 'month':  # 주봉 데이터 받기
                    if not self.objStockChart.req_chart_dwm(code[0], ord('M'), count, self, from_date, ohlcv_only):
                        continue
                df = pd.DataFrame(self.rcv_data, columns=columns, index=self.rcv_data['date'])

                # 기존 DB와 겹치는 부분 제거
                if from_date != 0:
                    df = df.loc[:from_date]
                    df = df.iloc[:-1]

                # 뒤집어서 저장 (결과적으로 date 기준 오름차순으로 저장됨)
                df = df.iloc[::-1]
                if tick_unit == 'day':
                    df.to_sql(code[0], self.sql_engine, if_exists='append', index_label='date', dtype={
                        'date': sqlalchemy.types.VARCHAR(20),
                        'open': sqlalchemy.types.BIGINT,
                        'high': sqlalchemy.types.BIGINT,
                        'low': sqlalchemy.types.BIGINT,
                        'close': sqlalchemy.types.BIGINT,
                        'volume': sqlalchemy.types.BIGINT,
                        'foreign_order_limit': sqlalchemy.types.VARCHAR(100),
                        'foreign_cur_qty': sqlalchemy.types.VARCHAR(100),
                        'foreign_cur_rate': sqlalchemy.types.VARCHAR(100),
                        'org_net_buying': sqlalchemy.types.VARCHAR(100),
                        'org_net_acc': sqlalchemy.types.VARCHAR(100)
                    })
                elif tick_unit == '1min':
                    df.to_sql('m' + code[0].lower(), self.sql_engine, if_exists='append', index_label='date', dtype={
                        'date': sqlalchemy.types.VARCHAR(20),
                        'open': sqlalchemy.types.BIGINT,
                        'high': sqlalchemy.types.BIGINT,
                        'low': sqlalchemy.types.BIGINT,
                        'close': sqlalchemy.types.BIGINT,
                        'volume': sqlalchemy.types.BIGINT
                    })

                # 메모리 overflow 방지
                del df
                gc.collect()


if __name__ == "__main__":
    cybos = CybosDataReaderCli()
    # cybos.update_price_db(tick_unit='day', ohlcv_only=False)
    cybos.update_price_db(tick_unit='day', tick_count=30, ohlcv_only=False)
    # cybos.code_name_db()
