import datetime as dt
import unicodedata


def is_market_open():
    now = dt.datetime.now()
    mmhh = int('{}{:02}'.format(now.hour, now.minute))
    if mmhh < 900 or mmhh > 1530:
        return False
    if now.weekday() >= 5:
        return False

    return True


def available_latest_date():
    now = dt.datetime.now()
    mmhh = int('{}{:02}'.format(now.hour, now.minute))
    # 장중인 경우에는 최신 데이터 연속적으로 발생하므로 None 반환
    if is_market_open():
        return None
    # 장중이 아닌 경우에 대해
    latest_date = now.replace(hour=15, minute=30)

    # 5 토요일 6: 일요일 주말인 경우 체크안함
    if now.weekday() >= 5:
        latest_date = latest_date - dt.timedelta(days=now.weekday() - 4)
        return convert_dt_to_int(latest_date)

    if mmhh > 1530:
        return convert_dt_to_int(latest_date)
    else:
        latest_date = latest_date - dt.timedelta(days=1)
        if latest_date.weekday() == 6:
            latest_date = latest_date - dt.timedelta(days=2)
        return convert_dt_to_int(latest_date)


def convert_dt_to_int(date_time):
    # 20220603 이런식으로
    return int(date_time.strftime("%Y%m%d%H%M"))


def preformat_cjk(string, width, align='<', fill=' '):
    count = (width - sum(1 + (unicodedata.east_asian_width(c) in "WF")
                         for c in string))
    return {
        '>': lambda s: fill * count + s,
        '<': lambda s: s + fill * count,
        '^': lambda s: fill * (count / 2)
                       + s
                       + fill * (count / 2 + count % 2)
    }[align](string)
