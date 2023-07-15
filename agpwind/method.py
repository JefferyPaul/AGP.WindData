import os
from datetime import datetime, date, timedelta
from typing import List

try:
    from WindPy import w
except:
    print('import WindPy error')
    raise ImportError

from agpwind.object import WindGeneralTickerInfoData, WindDailyBarData


def _parse_transaction_fee(s) -> (float, float):
    """
    wind trasactionfee eg:
        1元/手
        0.01%
    :param s:
    :return:
    """
    s = str(s)
    commission_on_rate = 0
    commission_per_share = 0

    # 数字，
    try:
        commission_on_rate = float(s)
    except :
        pass
    else:
        return commission_on_rate, commission_per_share
    # 百分比
    if '%' in s:
        if s[-2:] == '%%':
            commission_on_rate = float(s.strip('%')) / 100 / 100
        elif s[-1:] == '%':
            commission_on_rate = float(s.strip('%')) / 100
        else:
            raise ValueError
        return commission_on_rate, commission_per_share
    # 其他, 元/手
    if '元/手' in s:
        try:
            commission_per_share = float(s.strip('元/手'))
        except :
            raise ValueError
        else:
            return commission_on_rate, commission_per_share
    print(f'_convert_transaction_fee() 无法识别, {s}')
    raise ValueError


def start_wind():
    w.start()


def end_wind():
    w.end()


INNER_WIND_MAPPING_EXCHANGE_NAME = [
# ['Inner', 'Wind']
    ["SHFE", "SHF"],
    ["DCE", "DCE"],
    ["CZCE", "CZC"],
    ["INE", "INE"],
    ["CFFEX", "CFE"]
]

INNER_WIND_MAPPING_PRODUCT_NAME = [
# ['Inner', 'Wind']
    ["au2", "au"]
]


def inner_symbol_to_wind(s: str) -> str:
    s_split = s.split(".")

    # 识别为 {name}.{exchange}
    if len(s_split) == 2:
        _name = s_split[0]
        _exchange = s_split[1]

        # 转换 exchange name
        for n in range(len(INNER_WIND_MAPPING_EXCHANGE_NAME)):
            _inner_name = INNER_WIND_MAPPING_EXCHANGE_NAME[n][0]
            if _exchange == _inner_name:
                _exchange = INNER_WIND_MAPPING_EXCHANGE_NAME[n][1]
                break

        # 转换 symbol name
        for n in range(len(INNER_WIND_MAPPING_PRODUCT_NAME)):
            _inner_name = INNER_WIND_MAPPING_PRODUCT_NAME[n][0]
            if _name == _inner_name:
                _name = INNER_WIND_MAPPING_PRODUCT_NAME[n][1]
                break

        # 转换完成，返回
        return f"{_name}.{_exchange}"

    # 未能识别
    else:
        pass
    return s


def wind_symbol_name_to_inner(s: str) -> str:
    s_split = s.split(".")

    # 识别为 {name}.{exchange}
    if len(s_split) == 2:
        _name = s_split[0]
        _exchange = s_split[1]

        # 转换 exchange name
        for n in range(len(INNER_WIND_MAPPING_EXCHANGE_NAME)):
            _inner_name = INNER_WIND_MAPPING_EXCHANGE_NAME[n][1]
            if _exchange == _inner_name:
                _exchange = INNER_WIND_MAPPING_EXCHANGE_NAME[n][0]
                break

        # 转换 symbol name
        for n in range(len(INNER_WIND_MAPPING_PRODUCT_NAME)):
            _inner_name = INNER_WIND_MAPPING_PRODUCT_NAME[n][1]
            if _name == _inner_name:
                _name = INNER_WIND_MAPPING_PRODUCT_NAME[n][0]
                break

        # 转换完成，返回
        return f"{_name}.{_exchange}"

    # 未能识别
    else:
        pass
    return s


"""
获取交易所合约信息
w.wsd()
WindGeneralTickerInfo
"""


def get_wind_general_ticker_info(
        symbol: str,
        start_date: datetime or date = None,
        end_date: datetime or date = None,
) -> List[WindGeneralTickerInfoData]:
    """
    获取单个 期货合约或品种的 合约信息，
    """

    if not end_date:
        end_date: date = datetime.now().date()
    if not start_date:
        start_date: date = end_date - timedelta(days=10)

    # w.start()
    # w.wsd（codes, fields, beginTime, endTime, options）
    # 可以支持取 多品种单指标 或者 单品种多指标 的时间序列数据
    all_data = list()

    # 获取
    print(f'get_wind_general_ticker_info(), checking {symbol}, {str(start_date)}, {str(end_date)}')
    wsd_data = w.wsd(
        codes=symbol,
        fields=",".join([
            "trade_hiscode",    # 月合约代码
            # "ftdate",           # 开始交易日
            # "ftdate_new",       # 开始交易日
            # "lasttrade_date",   # 最后交易日
            # "ltdate_new",       # 最后交易日
            "transactionfee",   # 交易手续费，
            "todaypositionfee",  # 平今手续费
            "margin",           # 保证金率
            # "changelt",         # 涨跌幅限制
            # "changelt_new",     # 涨跌幅限制
            "punit",            # 报价单位
            "mfprice",          # 最小变动价位
            # "mfprice1",
            "contractmultiplier",   # 合约乘数
            # "thours2",          # 交易时间说明
        ]),
        beginTime=start_date,
        endTime=end_date,
        Fill='Previous',     # 默认为 "Blank"
        # usedf=True,
    )
    # """
    # usedf=False
    if wsd_data.ErrorCode == 0:
        pass
    else:
        print(f"Error Code: {wsd_data.ErrorCode}")
        print(f"Error Message: {wsd_data.Data[0][0]}")
        os.system('pause')
    # pprint(wsd_data, indent=4)
    # """
    """
    # usedf = True
    if wsd_data[0]:
        print(f"Error Message: {wsd_data[0]}")
    pprint(wsd_data, indent=4)
    df = wsd_data[1]        
    # print(df.head())
    """

    # 数据格式转换
    for n in range(len(wsd_data.Times)):
        _date = wsd_data.Times[n]
        _ticker = wsd_data.Data[0][n]
        _transaction_fee = str(wsd_data.Data[1][n])
        _transaction_fee_float_today = str(wsd_data.Data[2][n])
        _margin = float(wsd_data.Data[3][n]) / 100
        _price_unit = str(wsd_data.Data[4][n])
        _min_move = float(str(wsd_data.Data[5][n]).strip(_price_unit))       # 5 人民币元/吨
        _point_value = float(wsd_data.Data[6][n])

        # 佣金识别
        _commission_on_rate, _commission_per_share = _parse_transaction_fee(_transaction_fee)
        _commission_on_rate_today, _commission_per_share_today = _parse_transaction_fee(_transaction_fee_float_today)
        if _commission_on_rate != 0:
            _flat_today_discount = _commission_on_rate_today / _commission_on_rate
        else:
            if _commission_per_share == 0:
                _flat_today_discount = 1
            else:
                _flat_today_discount = _commission_per_share_today / _commission_per_share

        _ = WindGeneralTickerInfoData(
            product=wind_symbol_name_to_inner(symbol),
            ticker=wind_symbol_name_to_inner(_ticker),
            date=_date,
            point_value=_point_value,
            min_move=_min_move,
            commission_on_rate=_commission_on_rate,
            commission_per_share=_commission_per_share,
            flat_today_discount=_flat_today_discount,
            margin=_margin
        )
        all_data.append(_)

    return all_data


"""
获取行情日数据
w.wsd()
WindDailyBar
"""


def get_wind_daily_bar(
        symbol: str,
        start_date: datetime or date = None,
        end_date: datetime or date = None,
) -> List[WindDailyBarData]:

    if not end_date:
        end_date: date = datetime.now().date()
    if not start_date:
        start_date: date = end_date - timedelta(days=10)

    # w.start()
    # w.wsd（codes, fields, beginTime, endTime, options）
    # 可以支持取 多品种单指标 或者 单品种多指标 的时间序列数据
    all_data = list()

    # 获取
    print(f'get_wind_daily_bar(), checking {symbol}, {str(start_date)}, {str(end_date)}')
    wsd_data = w.wsd(
        codes=symbol,
        fields=",".join([
            "trade_hiscode",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amt",      # 成交额
            "oi",       # 持仓量
            "oiamount"  # 持仓额
        ]),
        beginTime=start_date,
        endTime=end_date,
        Fill='Previous',     # 默认为 "Blank"
        # usedf=True,
    )
    # """
    # usedf=False
    if wsd_data.ErrorCode == 0:
        pass
    else:
        print(f"Error Code: {wsd_data.ErrorCode}")
        print(f"Error Message: {wsd_data.Data[0][0]}")
        os.system('pause')
    # pprint(wsd_data, indent=4)
    # """
    """
    # usedf = True
    if wsd_data[0]:
        print(f"Error Message: {wsd_data[0]}")
    pprint(wsd_data, indent=4)
    df = wsd_data[1]        
    # print(df.head())
    """

    # 数据格式转换
    for n in range(len(wsd_data.Times)):
        _date = wsd_data.Times[n]
        _ticker = wsd_data.Data[0][n]
        _o = float(wsd_data.Data[1][n])
        _h = float(wsd_data.Data[2][n])
        _l = float(wsd_data.Data[3][n])
        _c = float(wsd_data.Data[4][n])
        _v = float(wsd_data.Data[5][n])
        _traded_values = float(wsd_data.Data[6][n])
        _open_interest = float(wsd_data.Data[7][n])
        _open_interest_value = float(wsd_data.Data[8][n])

        _ = WindDailyBarData(
            product=wind_symbol_name_to_inner(symbol),
            ticker=wind_symbol_name_to_inner(_ticker),
            date=_date,
            open=_o,
            high=_h,
            low=_l,
            close=_c,
            volume=_v,
            traded_value=_traded_values,
            open_interest=_open_interest,
            open_interest_value=_open_interest_value
        )
        all_data.append(_)

    return all_data


