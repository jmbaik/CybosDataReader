import cx_Oracle as oci
import pymysql


class MariaToOracleConvert:
    def __init__(self):
        self.ora_con = oci.connect('metstock/man100@192.168.60.152:1521/met12c.metel.com')
        self.my_con = pymysql.connect(host='localhost', user='metstock', password='man100', charset='utf8')
        self.dic_codes = {}
        self.code_names()

    def __del__(self):
        self.ora_con.close()
        self.my_con.close()

    def code_names(self):
        q = '''select `종목코드` as code, `종목명` as name from codex.code_name'''
        cur = self.my_con.cursor()
        cur.execute(q)
        res = cur.fetchall()
        for d in res:
            self.dic_codes.update({d[0]: d[1]})

    def select_m1(self, code):
        q = '''
                select date, nvl(open,0) as open, nvl(high,0) as high, nvl(low,0) as low, 
                    nvl(close,0) as close , nvl(volume,0) as volume 
                from codex.{0} order by date asc;
            '''.format(code)
        cur = self.my_con.cursor()
        cur.execute(q)
        return cur.fetchall()

    def merge_ora_m1(self, code):
        res = self.select_m1(code)
        var_list = []
        for r in res:
            date, open, high, low, close, volume = r
            name = self.dic_codes[code]
            var_list.append((code, date, name, open, high, low, close, volume))
        merge_sql = '''
                    MERGE INTO METSTOCK.CODEX_ALL_M1 d
                    USING (
                      Select
                        substr(:1, 2) as CODE, to_date(:2, 'YYYYMMDD') as DT, :3 as NAME, :4 as OPEN,
                        :5 as HIGH, :6 as LOW, :7 as CLOSE, :8 as VOLUME
                      From Dual) s
                    ON
                      (d.CODE = s.CODE and 
                      d.DT = s.DT )
                    WHEN MATCHED
                    THEN
                    UPDATE SET
                      d.NAME = s.NAME,  d.OPEN = s.OPEN,  d.HIGH = s.HIGH,  d.LOW = s.LOW,
                      d.CLOSE = s.CLOSE,  d.VOLUME = s.VOLUME,  d.UPDDT = sysdate
                    WHEN NOT MATCHED
                    THEN
                    INSERT (
                      CODE, DT, NAME,  OPEN, HIGH, LOW,  CLOSE, VOLUME, REGDT,  UPDDT)
                    VALUES (s.CODE, s.DT, s.NAME,  s.OPEN, s.HIGH, s.LOW,  s.CLOSE, s.VOLUME, sysdate,  sysdate)
                    '''
        cur = self.ora_con.cursor()
        cur.executemany(merge_sql, var_list)
        self.ora_con.commit()
        print(code, ' merge end')

    def proc_merge_all_m1(self):
        for code in self.dic_codes.keys():
            self.merge_ora_m1(code)
        print('proc_merge_all_m1 process end')


if __name__ == '__main__':
    m2 = MariaToOracleConvert()
    m2.code_names()
    # print(m2.dic_codes)
    # res = m2.select_m1('A005930')
    # m2.merge_ora_m1('A005930')
    m2.proc_merge_all_m1()
