import os
from collections import namedtuple
from datetime import datetime, date, timedelta
from typing import List, Dict
from dataclasses import dataclass


@dataclass
class WindGeneralTickerInfoData:
    product: str
    ticker: str
    date: datetime or date

    point_value: float  # 合约乘数。1张合约 的 “价值 / 报价”乘数，   value / share = price * point_value
    min_move: float  # 价格最小变动幅度；1个tick 对应的 价格变动数值.
    # lot_size: float  # 最少交易多少手 （的倍数）。 wind没有合格信息
    commission_on_rate: float  # 手续费，交易价值的比率
    commission_per_share: float  # 手续费，每张多少钱
    flat_today_discount: float  # 平今佣金倍率。1：相同；0：不收钱；2：收2倍
    margin: float   # 保证金率

    def __repr__(self):
        return (f'<WindGeneralTickerInfoData(product={self.product}, '
                f'date={str(self.date)}, ticker={self.ticker}, '
                f'point_value={self.point_value}, min_move={self.min_move}, '
                f'commission_on_rate={self.commission_on_rate}, commission_per_share={self.commission_per_share}, '
                f'flat_today_discount={self.flat_today_discount}, margin={self.margin}')

    def __str__(self):
        return ','.join([str(_) for _ in self.__dict__.values()])

    def __init__(
            self,
            product, ticker, date, point_value, min_move,
            commission_on_rate, commission_per_share, flat_today_discount, margin,
            **kwargs
    ):
        self.product = str(product)
        self.ticker = str(ticker)
        if type(date) is str:
            if '-' in date:
                self.date = datetime.strptime(date, "%Y-%m-%d").date()
            elif '/' in date:
                self.date = datetime.strptime(date, "%Y/%m/%d").date()
            else:
                self.date = datetime.strptime(date, "%Y%m%d").date()
        else:
            self.date = date
        self.point_value = float(point_value)
        self.min_move = float(min_move)
        self.commission_on_rate = float(commission_on_rate)
        self.commission_per_share = float(commission_per_share)
        self.flat_today_discount = float(flat_today_discount)
        self.margin = float(margin)

    def check(self) -> bool:
        _error_item = ['', None]
        if (self.commission_per_share in _error_item) and (self.commission_on_rate in _error_item):
            return False
        if self.margin in _error_item:
            return False
        if self.point_value in _error_item:
            return False
        if self.flat_today_discount in _error_item:
            return False
        if self.min_move in _error_item:
            return False
        return True


class WindGeneralTickerInfoFile:
    @classmethod
    def to_file(cls, data: List[WindGeneralTickerInfoData], path):
        assert type(data) == list
        try:
            output_root = os.path.dirname(path)
        except:
            raise NotADirectoryError
        if not os.path.isdir(output_root):
            try:
                os.makedirs(output_root)
            except:
                raise NotADirectoryError
        with open(path, 'w') as f:
            f.write(','.join(list(WindGeneralTickerInfoData.__annotations__.keys())) + '\n')
            f.writelines('\n'.join([str(_) for _ in data]))

    @classmethod
    def from_file(cls, path) -> List[WindGeneralTickerInfoData]:
        assert os.path.isfile(path)
        with open(path) as f:
            l_lines = f.readlines()
        if len(l_lines) == 0:
            return []
        l_data = []
        for line in l_lines[1:]:
            line = line.strip()
            if line == '':
                continue
            line_split = line.split(',')
            if len(line_split) != len(WindGeneralTickerInfoData.__annotations__.keys()):
                assert ValueError
            l_data.append(WindGeneralTickerInfoData(
                **dict(zip(WindGeneralTickerInfoData.__annotations__.keys(), line_split))))
        return l_data


@dataclass
class WindDailyBarData:
    product: str
    ticker: str
    date: datetime or date

    open: float
    high: float
    low: float
    close: float
    volume: float
    traded_value: float
    open_interest: float
    open_interest_value: float

    def __str__(self):
        return ','.join([str(_) for _ in self.__dict__.values()])

    def __repr__(self):
        return (f'<WindDailyBarData(product={self.product}, '
                f'date={str(self.date)}, ticker={self.ticker}, '
                f'open={self.open}, high={self.high}, low={self.low}, close={self.close}, '
                f'volume={self.volume}, traded_value={self.traded_value}, '
                f'open_interest={self.open_interest}, open_interest_value={self.open_interest_value}')

    def __init__(
            self,
            product, ticker, date, open, high, low, close, volume, traded_value, open_interest, open_interest_value,
            **kwargs
    ):
        self.product = str(product)
        self.ticker = str(ticker)
        if type(date) is str:
            if '-' in date:
                self.date = datetime.strptime(date, "%Y-%m-%d").date()
            elif '/' in date:
                self.date = datetime.strptime(date, "%Y/%m/%d").date()
            else:
                self.date = datetime.strptime(date, "%Y%m%d").date()
        else:
            self.date = date
        self.open = float(open)
        self.high = float(high)
        self.low = float(low)
        self.close = float(close)
        self.volume = float(volume)
        self.traded_value = float(traded_value)
        self.open_interest = float(open_interest)
        self.open_interest_value = float(open_interest_value)


class WindDailyBarFile:
    @classmethod
    def to_file(cls, data: List[WindDailyBarData], path):
        assert type(data) == list
        try:
            output_root = os.path.dirname(path)
        except:
            raise NotADirectoryError
        if not os.path.isdir(output_root):
            try:
                os.makedirs(output_root)
            except:
                raise NotADirectoryError
        with open(path, 'w') as f:
            f.write(','.join(list(WindDailyBarData.__annotations__.keys())) + '\n')
            f.writelines('\n'.join([str(_) for _ in data]))

    @classmethod
    def from_file(cls, path) -> List[WindDailyBarData]:
        assert os.path.isfile(path)
        with open(path) as f:
            l_lines = f.readlines()
        if len(l_lines) == 0:
            return []
        l_data = []
        for line in l_lines[1:]:
            line = line.strip()
            if line == '':
                continue
            line_split = line.split(',')
            if len(line_split) != len(WindDailyBarData.__annotations__.keys()):
                assert ValueError
            l_data.append(WindDailyBarData(
                **dict(zip(WindDailyBarData.__annotations__.keys(), line_split))))
        return l_data
