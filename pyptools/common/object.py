from datetime import date
from dataclasses import dataclass
from typing import Dict, List

import numpy as np

from pyptools.common.constant import *


"""
有些数据类，有多种形式，需要分别定义。
    例如 TradeData，有QM的、有OMS的、等等；GeneralTickerInfo，有 platinum使用的、有一般内部使用的等等。
此处定义的类，均是作为pyptools内部使用，与platinum使用的会有差异。
    例如：
    GeneralTickerInfo，platinum中的定义比较复杂，不适合内部使用。
上述情况，需要在  platinum_component中再次定义 platinum版本。


【Ticker, Product】  
没有重复实例

对于Ticker和Product，不使用 枚举类Exchange，而是用简单的 string表示，降低复杂度，
因为交易所/合约/品种可能随时增减，不能使用枚举类来使之被限制，
而且此处的exchange没有实质的作用，仅需要用string表示。
对于Ticker和Product，它们的Exchange的定义是： s.split(".")[-1]，没有其他更多的含义


"""


class Singleton:
    def __new__(cls, *args, **kwargs):
        if not getattr(cls, '_singleton', None):
            cls._singleton = super().__new__(cls, *args, **kwargs)
        return cls._singleton


class UnsetDict(dict):
    def __setitem__(self, key, value):
        print(f'{self.__class__.__name__} 不能直接修改键值，请使用 .set()')

    def set(self, key, value):
        super(UnsetDict, self).__setitem__(key, value)


class Ticker:
    """
    合约标的
    """
    _instances = {}
    count = 0

    def __new__(cls, symbol: str, exchange: str):
        if (symbol, exchange) in cls._instances.keys():
            pass
        else:
            _instance = super().__new__(cls)
            cls._instances[(symbol, exchange)] = _instance
            cls.count += 1
        return cls._instances[(symbol, exchange)]

    def __init__(self, symbol: str, exchange: str):
        self._symbol = symbol            # 合约名称（不包含交易所）
        self._exchange = exchange        # 交易所
        self._name = f'{self.symbol}.{self.exchange}'
        self._product_symbol = self._gen_product_symbol_from_ticker_symbol()
        self._product = Product(symbol=self._product_symbol, exchange=self.exchange)

    @property
    def symbol(self):
        return self._symbol

    @property
    def exchange(self):
        return self._exchange

    @property
    def name(self):
        return self._name

    @property
    def product(self):
        return self._product

    @classmethod
    def gen_obj_from_name(cls, name: str):
        # 通过 ticker_name 生成 Ticker对象
        if '.' in name:
            exchange = name.split('.')[-1]
            symbol = '.'.join(name.split('.')[:-1])
        else:
            exchange = ''
            symbol = name
        return cls(symbol=symbol, exchange=exchange)

    def _gen_product_symbol_from_ticker_symbol(self) -> str:
        # 从 ticker symbol 识别 product_name
        _num = 0
        for _num, s in enumerate(self.symbol[::-1]):
            if not str(s).isdigit():
                break
        product_name = self.symbol[:len(self.symbol)-_num]
        return product_name

    def __lt__(self, other):
        return self.name.lower() < other.name.lower()

    def __gt__(self, other):
        return bool(1 - self.__lt__(other))

    def __repr__(self):
        return f'Ticker(symbol={self.symbol},exchange={self.exchange})'

    def __str__(self):
        return f'Ticker:{self.name}'


class Product:
    """
    不提供 platinum里的 InternalProduct 功能，此信息直接通过GeneralTickerInfo.csv中查询相应product获取。
    """
    _instances = {}
    count = 0

    def __new__(cls, symbol: str, exchange: str):
        if (symbol, exchange) in cls._instances.keys():
            pass
        else:
            _instance = super().__new__(cls)
            cls._instances[(symbol, exchange)] = _instance
            cls.count += 1
        return cls._instances[(symbol, exchange)]

    def __init__(self, symbol: str, exchange: str):
        self._symbol = symbol
        self._exchange = exchange
        self._name = f'{self.symbol}.{self.exchange}'

    @property
    def symbol(self):
        return self._symbol

    @property
    def exchange(self):
        return self._exchange

    @property
    def name(self):
        return self._name

    @classmethod
    def gen_obj_from_name(cls, name):
        if '.' in name:
            exchange = name.split('.')[-1]
            symbol = '.'.join(name.split('.')[:-1])
        else:
            exchange = ''
            symbol = name
        return cls(symbol=symbol, exchange=exchange)

    def __lt__(self, other):
        return self.name.lower() < other.name.lower()

    def __gt__(self, other):
        return bool(1 - self.__lt__(other))

    def __repr__(self):
        return f'Product(symbol={self.symbol},exchange={self.exchange})'

    def __str__(self):
        return f'Product:{self.name}'


"""
    dataclass
"""


@dataclass(order=True)
class TradeData:
    datatime: datetime
    ticker: Ticker
    direction: Direction
    offset_flag: OffsetFlag
    price: float = 0
    volume: float = 0
    commission: float = 0

    def __str__(self):
        return ','.join([
            self.datatime.strftime("%Y%m%d %H%M%S.%f"),
            self.ticker.name,
            self.direction.name,
            self.offset_flag.name,
            str(self.price),
            str(self.volume),
            str(self.commission)
        ])


@dataclass(order=True)
class PositionData:
    datatime: datetime
    ticker: Ticker
    direction: Direction
    volume: float = 0
    volume_today: float = None
    price: float = 0

    def __str__(self):
        return ','.join([
            self.datatime.strftime('%Y%m%d %H%M%S'),
            self.ticker.name,
            self.direction.name,
            str(self.volume),
            str(self.volume_today) if self.volume_today else '',
            str(self.price),
        ])


@dataclass(order=True)
class AccountData:
    datatime: datetime
    account: str
    balance: float = 0
    available: float = 0
    risk_ration: float = 0

    def __str__(self):
        return ','.join([
            self.datatime.strftime('%Y%m%d %H%M%S'),
            self.account,
            str(self.balance),
            str(self.available),
            str(self.risk_ration),
        ])


@dataclass(order=True)
class TraderPnLData:
    # 对应QMReport.Pnl_.csv
    # 未确定
    datatime: datetime
    trader: str = ''
    pnl: float = 0
    commission: float = 0
    initX: float = np.nan

    def __str__(self):
        return ','.join([
            self.datatime.strftime('%Y%m%d %H%M%S'),
            self.trader,
            str(self.pnl),
            str(self.commission),
            str(self.initX),
        ])

    @classmethod
    def _get_header(cls) -> str:
        return ','.join(['date', 'trader', 'pnl', 'commission', 'initX'])


"""
Platinum.DS 数据
"""


@dataclass
class TickData:
    """"""

    ticker: Ticker
    date: date
    time: time

    # volume: float = 0
    # open_interest: float = 0
    # last_price: float = 0
    # last_volume: float = 0
    # limit_up: float = 0
    # limit_down: float = 0

    open: float
    high: float
    low: float
    close: float
    volume: float
    price: float

    bid_price_1: float = 0
    bid_price_2: float = 0
    bid_price_3: float = 0
    bid_price_4: float = 0
    bid_price_5: float = 0

    ask_price_1: float = 0
    ask_price_2: float = 0
    ask_price_3: float = 0
    ask_price_4: float = 0
    ask_price_5: float = 0

    bid_volume_1: float = 0
    bid_volume_2: float = 0
    bid_volume_3: float = 0
    bid_volume_4: float = 0
    bid_volume_5: float = 0

    ask_volume_1: float = 0
    ask_volume_2: float = 0
    ask_volume_3: float = 0
    ask_volume_4: float = 0
    ask_volume_5: float = 0

    localtime: time = None


@dataclass
class BarData:
    """
    Candlestick bar data of a certain trading period.
    """

    ticker: Ticker
    date: date
    time: time

    open: float
    high: float
    low: float
    close: float
    volume: float           # 成交量
    price: float            # 成交价格
    open_interest: float        # 合约持仓量

    interval: float = 60        # Bar数据的时间间隔


"""

"""


@dataclass
class GeneralTickerInfoData:
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




@dataclass
class TradingSessionData:
    Date: date              # 信息日期
    Product: Product
    TradingSession: List[List[time]]        #
    ExchangeTimezone: str           # 交易所所在时区，很少情况需要用到，所以作废（乱填）


class TradingSessionDataSet:
    def __init__(self, data):
        self._data: Dict[Product, List[TradingSessionData]] = data

    def get(self, product: Product, checking_date=datetime.today().date()) -> List[List[time]] or None:
        _product_ts_list: List[TradingSessionData] or None = self._data.get(product)
        if not _product_ts_list:
            return None
        else:
            if len(_product_ts_list) == 1:
                return _product_ts_list[0].TradingSession
            else:
                _nearest_ts = [_ts for _ts in _product_ts_list if _ts.Date <= checking_date]
                if _nearest_ts:
                    return max(_nearest_ts, key=lambda x: x.date).TradingSession
                else:
                    return min(_product_ts_list, key=lambda x: x.date).TradingSession

