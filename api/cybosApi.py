# coding=utf-8
import locale
import time
import os
import win32com.client
from pywinauto import application

locale.setlocale(locale.LC_ALL, 'ko_KR')

g_cybos_status = win32com.client.Dispatch('CpUtil.CpCybos')


# decorate
def check_plus_status(original_func):
    def wrapper(*args, **kwargs):
        b_connect = g_cybos_status.IsConnect
        if b_connect == 0:
            print('plus 가 정상적으로 연결 되지 않음')
            exit()
        return original_func(*args, **kwargs)

    return wrapper


class CybosChart:
    g_objCpStatus = None

    def __init__(self):
        self.g_objCpStatus = win32com.client.Dispatch('CpUtil.CpCybos')
        self.stock_chart = win32com.client.Dispatch('CpSysDib.StockChart')

    def kill_client(self):
        print("########## 기존 CYBOS 프로세스 강제 종료")
        os.system('taskkill /IM ncStarter* /F /T')
        os.system('taskkill /IM CpStart* /F /T')
        os.system('taskkill /IM DibServer* /F /T')
        os.system('wmic process where "name like \'%ncStarter%\'" call terminate')
        os.system('wmic process where "name like \'%CpStart%\'" call terminate')
        os.system('wmic process where "name like \'%DibServer%\'" call terminate')

    def connect(self, id_, pwd):
        if not self.connected():
            self.disconnect()
            self.kill_client()
            print("########## CYBOS 프로세스 자동 접속")
            app = application.Application()
            # cybos plus를 정보 조회로만 사용했기 때문에 인증서 비밀번호는 입력하지 않았다.
            app.start('C:\Daishin\Starter\\ncStarter.exe /prj:cp /id:{id} /pwd:{pwd} /autostart'.format(id=id_, pwd=pwd))

        while not self.connected():
            time.sleep(1)
        return True

    def connected(self):
        b_connected = self.g_objCpStatus.IsConnect
        if b_connected == 0:
            return False
        return True

    def disconnect(self):
        if self.connected():
            self.g_objCpStatus.PlusDisconnect()

    def waitForRequest(self):
        remainCount = self.g_objCpStatus.GetLimitRemainCount(1)
        if remainCount <= 0:
            time.sleep(self.g_objCpStatus.LimitRequestRemainTime / 1000)

    def _check_req_status(self):
        req_status = self.stock_chart.GetDibStatus()
        req_return = self.stock_chart.GetDibMsg1()
        if req_status == 0:
            pass
            # print("통신상태 정상[{}]{}".format(rqStatus, rqRet), end=' ')
        else:
            print("통신 상태 오류[{}]{} 종료합니다..".format(req_status, req_return))
            exit()

    @check_plus_status
    def req_chart_dwm(self, code, dwm, count, caller, from_date=0, ohlcv_only=True):
        """
        :param code: 종목코드
        :param dwm: 'D':일봉, 'W':주봉, 'M':월봉
        :param count: 요청할 데이터 개수
        :param caller: 이 메소드 호출한 인스턴스. 결과 데이터를 caller의 멤버로 전달하기 위함
        :return: None
        """
        self.stock_chart.SetInputValue(0, code)  # 종목코드
        self.stock_chart.SetInputValue(1, ord('2'))  # 개수로 받기
        self.stock_chart.SetInputValue(4, count)  # 최근 count 개
        if ohlcv_only:
            self.stock_chart.SetInputValue(5, [0, 2, 3, 4, 5, 8])  # 요청항목 - 날짜,시가,고가,저가,종가,거래량
            req_column = ('date', 'open', 'high', 'low', 'close', 'volume')
        else:
            # 요청항목
            self.stock_chart.SetInputValue(5, [0,  # 날짜
                                               2,  # 시가
                                               3,  # 고가
                                               4,  # 저가
                                               5,  # 종가
                                               8,  # 거래량
                                               12,  # 상장주식수
                                               14,  # 외국인주문한도수량
                                               16,  # 외국인현보유수량
                                               17,  # 외국인현보유비율
                                               20,  # 기관순매수
                                               21,  # 기관누적순매수
                                               ])
            # 요청한 항목들을 튜플로 만들어 사용
            '''
                상장주식수 : shares
                외국인한도수량 : foreign_order_limit
                외국인현보유수량 : foreign_cur_qty
                외국인현보유비율 : foreign_cur_rate
                기관순매수 : org_net_buying
                기관누적순매수 : org_net_acc
            '''
            req_column = ('date', 'open', 'high', 'low', 'close', 'volume',
                          'shares', 'foreign_order_limit', 'foreign_cur_qty', 'foreign_cur_rate', 'org_net_buying', 'org_net_acc')

        self.stock_chart.SetInputValue(6, dwm)  # '차트 주기 - 일/주/월
        self.stock_chart.SetInputValue(9, ord('1'))  # 수정 주가 사용

        rcv_data = {}
        for col in req_column:
            rcv_data[col] = []

        rcv_count = 0
        while count > rcv_count:
            self.stock_chart.BlockRequest()  # 요청! 후 응답 대기
            self._check_req_status()  # 통신상태 검사
            time.sleep(0.25)  # 시간당 RQ 제한으로 인해 장애가 발생하지 않도록 딜레이를 줌

            rcv_batch_len = self.stock_chart.GetHeaderValue(3)  # 받아온 데이터 개수
            rcv_batch_len = min(rcv_batch_len, count - rcv_count)  # 정확히 count 개수 만큼 받기 위함
            for i in range(rcv_batch_len):
                for col_idx, col in enumerate(req_column):
                    rcv_data[col].append(self.stock_chart.GetDataValue(col_idx, i))

            if len(rcv_data['date']) == 0:  # 데이터가 없는 경우
                print(code, '데이터 없음')
                return False

            # rcv_batch_len 만큼 받은 데이터의 가장 오래된 date
            rcv_oldest_date = rcv_data['date'][-1]

            rcv_count += rcv_batch_len
            caller.return_status_msg = '{} / {}'.format(rcv_count, count)

            # 서버가 가진 모든 데이터를 요청한 경우 break.
            # self.stock_chart.Continue 는 개수로 요청한 경우
            # count만큼 이미 다 받았더라도 계속 1의 값을 가지고 있어서
            # while 조건문에서 count > rcv_count를 체크해줘야 함.
            if not self.stock_chart.Continue:
                break
            if rcv_oldest_date < from_date:
                break
        caller.rcv_data = rcv_data
        return True

    @check_plus_status
    def req_chart_mt(self, code, dwm, tick_range, count, caller, from_date=0, ohlcv_only=True):
        """
                :param code: 종목 코드
                :param dwm: 'm':분봉, 'T':틱봉
                :param tick_range: 1분봉 or 5분봉, ...
                :param count: 요청할 데이터 개수
                :param caller: 이 메소드 호출한 인스턴스. 결과 데이터를 caller의 멤버로 전달하기 위함
                :return:
        """
        self.stock_chart.SetInputValue(0, code)  # 종목코드
        self.stock_chart.SetInputValue(1, ord('2'))  # 개수로 받기
        self.stock_chart.SetInputValue(4, count)  # 조회 개수
        if ohlcv_only:
            self.stock_chart.SetInputValue(5, [0, 1, 2, 3, 4, 5, 8])  # 요청항목 - 날짜, 시간,시가,고가,저가,종가,거래량
            rq_column = ('date', 'time', 'open', 'high', 'low', 'close', 'volume')
        else:
            # 요청항목
            self.stock_chart.SetInputValue(5, [0,  # 날짜
                                               1,  # 시간
                                               2,  # 시가
                                               3,  # 고가
                                               4,  # 저가
                                               5,  # 종가
                                               8,  # 거래량
                                               12,  # 상장주식수
                                               14,  # 외국인주문한도수량
                                               16,  # 외국인현보유수량
                                               17,  # 외국인현보유비율
                                               20,  # 기관순매수
                                               21,  # 기관누적순매수
                                               ])
            # 요청한 항목들을 튜플로 만들어 사용
            rq_column = ('date', 'time', 'open', 'high', 'low', 'close', 'volume',
                         '상장주식수', '외국인주문한도수량', '외국인현보유수량', '외국인현보유비율', '기관순매수', '기관누적순매수')

        self.stock_chart.SetInputValue(6, dwm)  # '차트 주기 - 분/틱
        self.stock_chart.SetInputValue(7, tick_range)  # 분틱차트 주기
        self.stock_chart.SetInputValue(9, ord('1'))  # 수정주가 사용

        rcv_data = {}
        for col in rq_column:
            rcv_data[col] = []

        rcv_count = 0
        while count > rcv_count:
            self.stock_chart.BlockRequest()  # 요청! 후 응답 대기
            self._check_req_status()  # 통신상태 검사
            time.sleep(0.25)  # 시간당 RQ 제한으로 인해 장애가 발생하지 않도록 딜레이를 줌

            rcv_batch_len = self.stock_chart.GetHeaderValue(3)  # 받아온 데이터 개수
            rcv_batch_len = min(rcv_batch_len, count - rcv_count)  # 정확히 count 개수만큼 받기 위함
            for i in range(rcv_batch_len):
                for col_idx, col in enumerate(rq_column):
                    rcv_data[col].append(self.stock_chart.GetDataValue(col_idx, i))

            if len(rcv_data['date']) == 0:  # 데이터가 없는 경우
                print(code, '데이터 없음')
                return False

            # len 만큼 받은 데이터의 가장 오래된 date
            rcv_oldest_date = int('{}{:04}'.format(rcv_data['date'][-1], rcv_data['time'][-1]))

            rcv_count += rcv_batch_len
            caller.return_status_msg = '{} / {}(maximum)'.format(rcv_count, count)

            # 서버가 가진 모든 데이터를 요청한 경우 break.
            # self.stock_chart.Continue 는 개수로 요청한 경우
            # count만큼 이미 다 받았더라도 계속 1의 값을 가지고 있어서
            # while 조건문에서 count > rcv_count를 체크해줘야 함.
            if not self.stock_chart.Continue:
                break
            if rcv_oldest_date < from_date:
                break

        # 분봉의 경우 날짜와 시간을 하나의 문자열로 합친 후 int로 변환
        rcv_data['date'] = list(map(lambda x, y: int('{}{:04}'.format(x, y)), rcv_data['date'], rcv_data['time']))
        del rcv_data['time']
        caller.rcv_data = rcv_data  # 받은 데이터를 caller의 멤버에 저장
        return True


class CybosCodeMgr:
    def __init__(self):
        self.code_mgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")

    # 마켓에 해당하는 종목코드 리스트 반환하는 메소드
    def get_code_list(self, market):
        """
        :param market: 1:코스피, 2:코스닥, ...
        :return: market에 해당하는 코드 list
        """
        code_list = self.code_mgr.GetStockListByMarket(market)
        return code_list

    # 부구분코드를 반환하는 메소드
    def get_section_code(self, code):
        section_code = self.code_mgr.GetStockSectionKind(code)
        return section_code

    # 종목 코드를 받아 종목명을 반환하는 메소드
    def get_code_name(self, code):
        code_name = self.code_mgr.CodeToName(code)
        return code_name
