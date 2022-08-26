# coding=utf-8
import gc
import pandas as pd
import pymysql
import sqlalchemy
from datetime import datetime


class MetStockApi:
    def __init__(self):
        sqlalchemy_database_uri = 'mysql+mysqlconnector://metstock:man100@localhost:3306/codex'
        self.conn = pymysql.connect(host='localhost', user='metstock', password='man100', db='codex', charset='utf8')
        self.sql_engine = sqlalchemy.create_engine(sqlalchemy_database_uri, echo=False, encoding='utf-8')
        self.code_dict = {}
        sql = "select cast(substring(`종목코드`, 2) as char) as code, `종목명` as code_nm from codex.code_name"
        with self.sql_engine.connect() as con:
            rs = con.execute(sql)
            for row in rs:
                self.code_dict[row[1]] = row[0]

    def __del__(self):
        self.conn.close()

    def get_chart_day(self, code=None, start=None, end=None):
        if code is None:
            return
        sql = "select * from codex.a%s where date between '%s' and '%s'" % (code, start, end)
        # return pd.read_sql(sql, self.sql_engine, parse_dates=[{'date': '%Y%m%d'}], index_col=['date'])
        df = pd.read_sql(sql, self.sql_engine)
        df['date'] = df['date'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
        df.set_index(df['date'], inplace=True)
        df = df.drop('date', axis=1)
        return df

    def get_nm_chart_day(self, code_nm=None, start=None, end=None):
        if code_nm not in self.code_dict:
            return
        return self.get_chart_day(code=self.code_dict[code_nm], start=start, end=end)

    def get_chart_1m(self, code=None, start=None, end=None):
        if code is None:
            return
        sql = "select * from codex.ma%s where date between '%s0901' and '%s1600'" % (code, start, end)
        # return pd.read_sql(sql, self.sql_engine, parse_dates=[{'date': '%Y%m%d'}], index_col=['date'])
        df = pd.read_sql(sql, self.sql_engine)
        df['date'] = df['date'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d%H%M'))
        df.set_index(df['date'], inplace=True)
        df = df.drop('date', axis=1)
        return df

    def get_chart_1m_ymd(self, code=None, ymd=None):
        if code is None:
            return
        sql = "select * from codex.ma%s where date between '%s0901' and '%s1600'" % (code, ymd, ymd)
        # return pd.read_sql(sql, self.sql_engine, parse_dates=[{'date': '%Y%m%d'}], index_col=['date'])
        df = pd.read_sql(sql, self.sql_engine)
        df['date'] = df['date'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d%H%M'))
        df.set_index(df['date'], inplace=True)
        df = df.drop('date', axis=1)
        return df

    def get_code_nm(self, code=None):
        return self.code_dict[code]


if __name__ == '__main__':
    metstock = MetStockApi()
    df = metstock.get_chart_1m(code='000020', start='20220501', end='20220610')
    df.info()
    code_nm = metstock.get_code_nm(code='000020')
    df2 = metstock.get_chart_1m_ymd(code='000020', ymd='20220502')
