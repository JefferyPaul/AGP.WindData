import os
from datetime import datetime, date, timedelta
from typing import List
import logging
from collections import defaultdict
import math

logger = logging.getLogger('apgwind')

try:
    from WindPy import w
except:
    logger.error('import WindPy error')
    raise ImportError

from agpwind.object import WindGeneralTickerInfoData, WindDailyBarData, WindMinuteBarData, WindMinuteBarFile


def _parse_transaction_fee(s) -> (float, float):
    """
    识别佣金规则，返回 (commission_on_rate, commission_per_share)
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
    logger.error(f'_parse_transaction_fee() 无法识别, {s}')
    raise ValueError


def start_wind():
    w.start()


def close_wind():
    w.close()


# 用于转换 exchange名字 的对应表
# ['Inner', 'Wind']
INNER_WIND_MAPPING_EXCHANGE_NAME = [
    ["SHFE", "SHF"],
    ["DCE", "DCE"],
    ["CZCE", "CZC"],
    ["INE", "INE"],
    ["CFFEX", "CFE"],
    ["GFEX", "GFE"]
]

# 用于转换 symbol名字 的对应表
# ['Inner', 'Wind']
INNER_WIND_MAPPING_PRODUCT_NAME = [
    ["au2", "au"]
]


def _inner_symbol_to_wind(s: str) -> str:
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


def _wind_symbol_name_to_inner(s: str) -> str:
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
        start_date: datetime or date = datetime.now().date(),
        end_date: datetime or date = datetime.now().date(),
) -> List[WindGeneralTickerInfoData]:
    """
    获取单个 期货合约或品种的 合约信息，
    """

    # w.start()
    # w.wsd（codes, fields, beginTime, endTime, options）
    # 可以支持取 多品种单指标 或者 单品种多指标 的时间序列数据
    all_data = list()

    # 获取
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
        logger.error(f"Error Code: {wsd_data.ErrorCode}")
        logger.error(f"Error Message: {wsd_data.Data[0][0]}")
        os.system('pause')
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
        _transaction_fee = wsd_data.Data[1][n]
        _transaction_fee_float_today = wsd_data.Data[2][n]
        _margin = float(wsd_data.Data[3][n]) / 100
        _price_unit = wsd_data.Data[4][n]           # 价格单位
        _min_move = float(str(wsd_data.Data[5][n]).strip(_price_unit))       # 5 人民币元/吨
        _point_value = float(wsd_data.Data[6][n])

        # 佣金识别
        if not _transaction_fee:
            _commission_on_rate, _commission_per_share = ('', '')
        else:
            _commission_on_rate, _commission_per_share = _parse_transaction_fee(_transaction_fee)
        if not _transaction_fee_float_today:
            _commission_on_rate_today, _commission_per_share_today = ('', '')
        else:
            _commission_on_rate_today, _commission_per_share_today = _parse_transaction_fee(_transaction_fee_float_today)
        if _commission_on_rate or _commission_per_share:
            if _commission_on_rate != 0:
                _flat_today_discount = round(_commission_on_rate_today / _commission_on_rate, 4)
            else:
                if _commission_per_share == 0:
                    _flat_today_discount = 1
                else:
                    _flat_today_discount = round(_commission_per_share_today / _commission_per_share, 4)
        else:
            _flat_today_discount = ''
        _ = WindGeneralTickerInfoData(
            product=_wind_symbol_name_to_inner(symbol),
            ticker=_wind_symbol_name_to_inner(_ticker),
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
    logger.info(f'get_wind_daily_bar(), checking {symbol}, {str(start_date)}, {str(end_date)}')
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
        logger.error(f"Error Code: {wsd_data.ErrorCode}")
        logger.error(f"Error Message: {wsd_data.Data[0][0]}")
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
        try:
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
        except Exception as e:
            logger.error('解析 wind_data 失败')
            logger.error(e)
            # logger.error(wsd_data.Data[1][n])
        else:
            _ = WindDailyBarData(
                product=_wind_symbol_name_to_inner(symbol),
                ticker=_wind_symbol_name_to_inner(_ticker),
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


def get_wind_minute_bar(
        symbol: str,
        start_date: datetime or date = None,
        end_date: datetime or date = None,
) -> List[WindMinuteBarData]:

    if not end_date:
        end_date: date = datetime.now().date()
    if not start_date:
        start_date: date = end_date - timedelta(days=10)

    # w.start()
    # w.wsd（codes, fields, beginTime, endTime, options）
    # 可以支持取 多品种单指标 或者 单品种多指标 的时间序列数据
    all_data = list()

    # 获取
    logger.info(f'get_wind_minute_bar(), checking {symbol}, {str(start_date)}, {str(end_date)}')

    wsi_data = w.wsi(
        codes=symbol,
        fields=",".join([
            "open",
            "high",
            "low",
            "close",
            "volume",
            "oi",       # 持仓量
        ]),
        beginTime=start_date,
        endTime=end_date,
        Fill='',     # 默认为 "Blank"
    )

    # """
    # usedf=False
    if wsi_data.ErrorCode == 0:
        pass
    else:
        logger.error(f"Error Code: {wsi_data.ErrorCode}")
        logger.error(f"Error Message: {wsi_data.Data[0][0]}")
        os.system('pause')

    # 数据格式转换
    for n in range(len(wsi_data.Times)):
        try:
            _data_time = wsi_data.Times[n] + timedelta(minutes=1)
            _o = float(wsi_data.Data[0][n])
            _h = float(wsi_data.Data[1][n])
            _l = float(wsi_data.Data[2][n])
            _c = float(wsi_data.Data[3][n])
            _v = float(wsi_data.Data[4][n])
            _open_interest = float(wsi_data.Data[5][n])
        except Exception as e:
            logger.error('解析 wind_data 失败')
            logger.error(e)
            # logger.error(wsd_data.Data[1][n])
        else:
            if True in [math.isnan(__) for __ in [_o, _h, _l, _c, _v]]:
                continue
            else:
                _ = WindMinuteBarData(
                    ticker=_wind_symbol_name_to_inner(symbol),
                    datatime=_data_time,
                    open=_o,
                    high=_h,
                    low=_l,
                    close=_c,
                    volume=_v,
                    open_interest=_open_interest,
                )
                all_data.append(_)

    return all_data


def output_wind_minute_bar_data(data: List[WindMinuteBarData], output_root):
    if not os.path.isdir(output_root):
        os.makedirs(output_root)
    l_data_group_by = defaultdict(list)
    for _ in data:
        l_data_group_by[(_.ticker, _.datatime.date().strftime('%Y%m%d'))].append(_)

    for _gb_key, _data_list in l_data_group_by.items():
        _ticker = _gb_key[0]
        _ticker_inner = _wind_symbol_name_to_inner(_ticker)
        _date = _gb_key[1]
        logger.info(f'output_wind_minute_bar_data, {_ticker_inner}, {_date}')

        _output_date_folder = os.path.join(output_root, _date)
        if not os.path.isdir(_output_date_folder):
            os.makedirs(_output_date_folder)
        _output_file = os.path.join(_output_date_folder, _ticker + '.csv')
        WindMinuteBarFile.to_file(data=_data_list, path=_output_file)

