"""
GeneralTickerInfo.csv

- TickerInfoData
  数据类型,存储数据.
    product: Product
    prefix: str  # Futures / Stock / Options / ...
    currency: str
    point_value: float  # 1张合约 的 “价值 / 报价”乘数，   value / share = price * point_value
    min_move: float  # 价格最小变动幅度；1个tick 对应的 价格变动数值
    lot_size: float  # 最少交易多少手 （的倍数），1手 是多少 张(shares)
    commission_on_rate: float  # 手续费，交易价值的比率
    commission_per_share: float  # 手续费，每张多少钱
    slippage_points: float
    flat_today_discount: float  # 平今佣金倍率。1：相同；0：不收钱；2：收2倍
    margin: float  # 保证金率
- GeneralTickerInfoFile
  .read() 文件读取, -> Dict[Product, TickerInfoData]
- GeneralTickerInfoManager
  输入目录, 如 "./Platinum/Platinum.Ds/Release/Data", 查找该目录下的 文件夹, 文件夹名作为 time zone index
  作为在Platinum组件中使用的用于管理GeneralTickerInfo的工具
"""

import os
from dataclasses import dataclass
from typing import Dict

from pyptools.common.object import Product
from pyptools.common.object import GeneralTickerInfoData
from pyptools.common.utility import read_data_file_with_func


# @dataclass
# class GeneralProductInfo:
#     """
#     eg:
#         Adapter,CTP
#         InternalProduct,DLi
#         Exchange,DCE
#         Prefix,Futures
#         TradingExchangeZoneIndex,210
#         Currency,CNY
#
#         PointValue,100
#         MinMove,0.5
#         LotSize,1
#         ExchangeRateXxxUsd,0.15
#         CommissionOnRate,0.0001
#          CommissionPerShareInXxx,0
#          MinCommissionInXxx,0
#          MaxCommissionInXxx,10000
#          StampDutyRate,0
#
#          SlippagePoints,0.5
#         Product,i
#         FlatTodayDiscount,1
#         Margin,10
#         IsLive,TRUE
#
#     """
#     adapter: str    # CTP / ZD
#     internal_product: str   # 内部名字
#     exchange: str
#     prefix: str  # Futures / Stock / Options / ...
#
#     trading_exchange_zone_index: str    # 所在时区，210
#     currency: str       # 计价货币
#
#     point_value: float  # 1张合约 的 “价值 / 报价”乘数，   value / share = price * point_value
#     min_move: float  # 价格最小变动幅度；1个tick 对应的 价格变动数值
#     lot_size: float  # 最少交易多少手 （的倍数），1手 是多少 张(shares)
#
#     exchange_rate_x_usd: float   # 美元汇率
#     commission_on_rate: float  # 手续费，交易价值的比率
#     commission_per_share: float  # 手续费，每张多少钱
#     min_commission_in_x: float    # 单张手续费，最低值
#     max_commission_in_x: float    # 单张手续费，最高值
#     stamp_duty_rate: float  # 印花税税率
#
#     slippage_points: float  # 滑点
#     product_symbol: str   #
#     flat_today_discount: float  # 平今佣金倍率。1：相同；0：不收钱；2：收2倍
#     margin: float  # 保证金率
#     is_live: bool   # 标的是否仍在上市中
#
#     def __post_init__(self):
#         self.product = Product(symbol=self.product_symbol, exchange=self.exchange)
#
#     def __str__(self):
#         return f'ProductInfoData:{str(self.product)}'


# @dataclass
# class TradingSession:
#     """
#     Date,ProductInfo,DaySession,NightSession,ExchangeTimezone
#     20010101,AP.CZCE,090000-101500&103000-113000&133000-150000,,210
#     """
#     date: datetime or date
#     product_info: Product
#     day_session: str
#     night_session: str
#     exchange_timezone: str
#
#     def __str__(self):
#         return f"TradingSession:{str(self.product_info)}"


# class GeneralProductInfoFile:
#     FileName = 'GeneralTickerInfo.csv'
#
#     @classmethod
#     def parse_a_line(cls, s: str) -> GeneralProductInfo:
#         split_line = s.split(',')
#         if len(split_line) != 20:
#             raise ValueError
#         return GeneralProductInfo(
#             adapter=split_line[0],
#             internal_product=split_line[1],
#             exchange=split_line[2],
#             prefix=split_line[3],
#             trading_exchange_zone_index=split_line[4],
#             currency=split_line[5],
#             point_value=float(split_line[6]),
#             min_move=float(split_line[7]),
#             lot_size=float(split_line[8]),
#             exchange_rate_x_usd=float(split_line[9]),
#             commission_on_rate=float(split_line[10]),
#             commission_per_share=float(split_line[11]),
#             min_commission_in_x=float(split_line[12]),
#             max_commission_in_x=float(split_line[13]),
#             stamp_duty_rate=float(split_line[14]),
#             slippage_points=float(split_line[15]),
#             product_symbol=split_line[16],
#             flat_today_discount=float(split_line[17]),
#             margin=float(split_line[18]),
#             is_live=bool(split_line[19])
#         )
#
#     @classmethod
#     def read(cls, path) -> List[GeneralProductInfo]:
#         assert os.path.isfile(path)
#         return _read_data_file_with_func(path, cls.parse_a_line, is_header=True)
#
#
# class GeneralProductInfoManager:
#
#     def __init__(self, path):
#         self._file_path = path
#         self._original_data: List[GeneralProductInfo] = GeneralProductInfoFile.read(path)
#         self._data: Dict[Product, GeneralProductInfo] = dict()
#         for _product_info in self._original_data:
#             if _product_info.product in self._data:
#                 raise KeyError
#             self._data[_product_info.product] = _product_info
#
#     @property
#     def data(self):
#         return self._data
#
#     def get_product_info(self, product: Product) -> GeneralProductInfo or None:
#         return self._data.get(product)


class GeneralTickerInfoFile:
    FileName = 'GeneralTickerInfo.csv'
    Header = 'Adapter,InternalProduct,Exchange,Prefix,TradingExchangeZoneIndex,Currency,' \
             'PointValue,MinMove,LotSize,ExchangeRateXxxUsd,CommissionOnRate,CommissionPerShareInXxx,' \
             'MinCommissionInXxx,MaxCommissionInXxx,StampDutyRate,' \
             'SlippagePoints,Product,FlatTodayDiscount,Margin,IsLive'

    @classmethod
    def read(cls, p,) -> Dict[Product, GeneralTickerInfoData]:
        assert os.path.isfile(p)
        d_ticker_infos = {}

        with open(p) as f:
            l_lines = f.readlines()
        l_lines = [_.strip() for _ in l_lines if _.strip()]
        if len(l_lines) <= 1:
            return d_ticker_infos

        for line in l_lines[1:]:
            _line_split = line.split(',')
            assert len(_line_split) == 20
            _product_symbol = _line_split[16]
            _exchange = _line_split[2]
            _product = Product(symbol=_product_symbol, exchange=_exchange)
            _product_info_data = GeneralTickerInfoData(
                product=_product,
                prefix=_line_split[3],
                currency=_line_split[5],
                point_value=float(_line_split[6]),
                min_move=float(_line_split[7]),
                lot_size=float(_line_split[8]),
                commission_on_rate=float(_line_split[10]),
                commission_per_share=float(_line_split[11]),
                slippage_points=float(_line_split[15]),
                flat_today_discount=float(_line_split[17]),
                margin=float(_line_split[18]),
            )
            d_ticker_infos[_product] = _product_info_data
        return d_ticker_infos


class GeneralTickerInfoManager:
    """
    一般每个platinum工具都需要 GeneralTickerInfo 信息

    读取，
        输入目录, 如 "C:/D/_workspace/Platinum/Platinum.Ds/Release/Data", 查找该目录下的 文件夹, 文件夹名作为 time zone index
    获取，

    """

    def __init__(self, path):
        self._data = {}
        self._set(path)

    @property
    def data(self):
        return self._data.copy()

    def _set(self, path):
        assert os.path.isdir(path)
        for _name in os.listdir(path):
            path_sub = os.path.join(path, _name)
            path_gti_file = os.path.join(path_sub, 'GeneralTickerInfo.csv')
            if not os.path.isfile(path_gti_file):
                continue
            else:
                try:
                    _gti: Dict[Product, GeneralTickerInfoData] = GeneralTickerInfoFile.read(path_gti_file)
                except Exception as e:
                    raise e
                else:
                    _time_zone_index = '.'.join(_name.split('.')[1:])
                    self._data[_time_zone_index] = _gti

    def get(self, product, time_zone_index='210') -> GeneralTickerInfoData or None:
        return self._data[time_zone_index][product]

    def get_time_zone_data(self, time_zone_index='210') -> Dict[Product, GeneralTickerInfoData] or None:
        return self._data[time_zone_index]
