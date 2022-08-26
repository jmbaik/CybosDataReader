import gc
import time

import pandas as pd
import sqlalchemy
import tqdm

from api import cybosApi
from utils import preformat_cjk


class CybosDataReaderCodex:
    def __init__(self):
        sqlalchemy_database_uri = 'mysql+mysqlconnector://metstock:man100@localhost:3306/codex'
        self.sql_engine = sqlalchemy.create_engine(sqlalchemy_database_uri, echo=False, encoding='utf-8')
        self.stockChartApi = cybosApi.CybosChart()
        if self.connected() is False:
            self.stockChartApi.connect('METEL100', '!pjmman100')
        self.codeMgrApi = cybosApi.CybosCodeMgr()
        self.rcv_data = dict()
        self.db_code_list = []
        self.code_list = []
        self.set_api()

    def connected(self):
        return self.stockChartApi.connected()

    def set_api(self):
        self.code_list = self.api_code_list()

    def api_code_list(self):
        return self.codeMgrApi.get_code_list(1) + self.codeMgrApi.get_code_list(2)

    def update_price_m1(self, tick_unit='1min', tick_count=None):
        self.db_code_list = []
        for code in self.code_list:
            self.db_code_list.append('m' + str(code).lower())

        columns = ['open', 'high', 'low', 'close', 'volume']
        sss = f"SELECT TABLE_NAME FROM information_schema.TABLES where TABLE_SCHEMA='codex' and TABLE_NAME not in ('company_info', 'daily_price')" \
              f" and length(table_name) = 8"
        sdf = pd.read_sql(sss, self.sql_engine)
        ma_code_list = sdf['TABLE_NAME'].values.tolist()
        tqdm_range = tqdm.trange(len(self.db_code_list), ncols=100)
        for i in tqdm_range:
            # code = self.db_code_list.iloc[i]
            code = self.code_list[i]
            db_code = self.db_code_list[i]
            update_status_msg = '[{}]'.format(code)
            if db_code in ma_code_list:
                result = self.sql_engine.execute("SELECT date FROM {} ORDER BY date DESC LIMIT 1".format(db_code))
                from_date = result.fetchall()[0][0]
            else:
                from_date = 0

            tqdm_range.set_description(preformat_cjk(update_status_msg, 25))
            if not self.stockChartApi.req_chart_mt(code, ord('m'), 1, tick_count, self, from_date, ohlcv_only=True):
                continue
            df = pd.DataFrame(self.rcv_data, columns=columns, index=self.rcv_data['date'])
            if from_date != 0:
                df = df.loc[:from_date]
                df = df.iloc[:-1]

            df = df.iloc[::-1]
            df.to_sql(db_code, self.sql_engine, if_exists='append', index_label='date', dtype={
                'date': sqlalchemy.types.VARCHAR(20),
                'open': sqlalchemy.types.BIGINT,
                'high': sqlalchemy.types.BIGINT,
                'low': sqlalchemy.types.BIGINT,
                'close': sqlalchemy.types.BIGINT,
                'volume': sqlalchemy.types.BIGINT
            })
            del df
            gc.collect()

    def update_price_tick(self, tick_unit='tick', tick_count=None):
        self.db_code_list = []
        for code in self.code_list:
            self.db_code_list.append('tick' + str(code).lower())

        columns = ['open', 'high', 'low', 'close', 'volume']
        sss = f"SELECT TABLE_NAME FROM information_schema.TABLES where TABLE_SCHEMA='codex' and TABLE_NAME not in ('company_info', 'daily_price')" \
              f" and length(table_name) = 11"
        sdf = pd.read_sql(sss, self.sql_engine)
        tick_code_list = sdf['TABLE_NAME'].values.tolist()
        tqdm_range = tqdm.trange(len(self.db_code_list), ncols=100)
        for i in tqdm_range:
            # code = self.db_code_list.iloc[i]
            code = self.code_list[i]
            db_code = self.db_code_list[i]
            update_status_msg = '[{}]'.format(code)
            if db_code in tick_code_list:
                result = self.sql_engine.execute("SELECT date FROM {} ORDER BY date DESC LIMIT 1".format(db_code))
                from_date = result.fetchall()[0][0]
            else:
                from_date = 0

            tqdm_range.set_description(preformat_cjk(update_status_msg, 25))
            if not self.stockChartApi.req_chart_mt(code, ord('T'), 1, tick_count, self, from_date, ohlcv_only=True):
                continue
            df = pd.DataFrame(self.rcv_data, columns=columns, index=self.rcv_data['date'])
            if from_date != 0:
                df = df.loc[:from_date]
                df = df.iloc[:-1]

            df = df.iloc[::-1]
            df.to_sql(db_code, self.sql_engine, if_exists='append', index_label='date', dtype={
                'date': sqlalchemy.types.VARCHAR(20),
                'open': sqlalchemy.types.BIGINT,
                'high': sqlalchemy.types.BIGINT,
                'low': sqlalchemy.types.BIGINT,
                'close': sqlalchemy.types.BIGINT,
                'volume': sqlalchemy.types.BIGINT
            })
            del df
            gc.collect()

    def update_price_day(self, tick_unit='day', tick_count=None):
        self.db_code_list = []
        for code in self.code_list:
            self.db_code_list.append(str(code).lower())
        columns = ['open', 'high', 'low', 'close', 'volume',
                   'shares', 'foreign_order_limit', 'foreign_cur_qty', 'foreign_cur_rate', 'org_net_buying', 'org_net_acc']
        sss = f"SELECT TABLE_NAME FROM information_schema.TABLES where TABLE_SCHEMA='codex' and TABLE_NAME not in ('company_info', 'daily_price')" \
              f" and length(table_name) = 7"
        sdf = pd.read_sql(sss, self.sql_engine)
        a_code_list = sdf['TABLE_NAME'].values.tolist()
        tqdm_range = tqdm.trange(len(self.code_list), ncols=100)
        for i in tqdm_range:
            # code = self.db_code_list.iloc[i]
            code = self.code_list[i]
            db_code = self.db_code_list[i]
            update_status_msg = '[{}]'.format(code)
            from_date = 0
            if db_code in a_code_list:
                result = self.sql_engine.execute("SELECT date FROM {} ORDER BY date DESC LIMIT 1".format(db_code))
                rows = result.fetchall()
                if len(rows) > 0:
                    from_date = rows[0][0]
            else:
                from_date = 0

            tqdm_range.set_description(preformat_cjk(update_status_msg, 25))
            if not self.stockChartApi.req_chart_dwm(code, ord('D'), tick_count, self, from_date, ohlcv_only=False):
                continue

            df = pd.DataFrame(self.rcv_data, columns=columns, index=self.rcv_data['date'])
            if from_date != 0:
                df = df.loc[:from_date]
                df = df.iloc[:-1]

            df = df.iloc[::-1]
            df.to_sql(db_code, self.sql_engine, if_exists='append', index_label='date', dtype={
                'date': sqlalchemy.types.VARCHAR(20),
                'open': sqlalchemy.types.BIGINT,
                'high': sqlalchemy.types.BIGINT,
                'low': sqlalchemy.types.BIGINT,
                'close': sqlalchemy.types.BIGINT,
                'volume': sqlalchemy.types.BIGINT,
                'shares': sqlalchemy.types.VARCHAR(100),
                'foreign_order_limit': sqlalchemy.types.VARCHAR(100),
                'foreign_cur_qty': sqlalchemy.types.VARCHAR(100),
                'foreign_cur_rate': sqlalchemy.types.VARCHAR(100),
                'org_net_buying': sqlalchemy.types.VARCHAR(100),
                'org_net_acc': sqlalchemy.types.VARCHAR(100)
            })
            del df
            gc.collect()


if __name__ == '__main__':
    reader = CybosDataReaderCodex()
    # reader.update_price_m1(tick_count=1000)
    # reader.update_price_m1(tick_count=1000)
    # reader.update_price_day(tick_count=5)
    # reader.update_price_tick(tick_count=5000)
    if reader.connected() is True:
        # reader.update_price_m1(tick_count=1000)
        reader.update_price_day(tick_count=5)
        reader.update_price_tick(tick_count=5000)
