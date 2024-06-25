import os
from collections import namedtuple
from datetime import datetime, date, timedelta
from typing import List, Dict
from dataclasses import dataclass, asdict
import logging
import json

logger = logging.getLogger('apgwind')


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

    def json(self):
        return {
            _k: str(_v)
            for _k, _v in self.__dict__.items()
        }

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
        try:
            self.min_move = float(min_move)
        except:
            logger.error(f'error min_move, {min_move}')
            raise Exception

        if commission_on_rate:
            try:
                self.commission_on_rate = float(commission_on_rate)
            except:
                logger.error(f'error commission_on_rate, {commission_on_rate}')
                raise Exception
        else:
            self.commission_on_rate = 0
        if commission_per_share:
            try:
                self.commission_per_share = float(commission_per_share)
            except:
                logger.error(f'error commission_per_share, {commission_per_share}')
                raise Exception
        else:
            self.commission_per_share = 0
        if flat_today_discount:
            try:
                self.flat_today_discount = float(flat_today_discount)
            except:
                logger.error(f'error flat_today_discount, {flat_today_discount}')
                raise Exception
        else:
            self.flat_today_discount = 0

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


@dataclass
class WindMinuteBarData:
    ticker: str
    datatime: datetime

    open: float
    high: float
    low: float
    close: float
    last: float
    volume: float
    open_interest: float

    def __str__(self):
        return ','.join([
            self.datatime.strftime('%H:%M:%S'),
            str(self.open),
            str(self.high),
            str(self.low),
            str(self.close),
            str(self.volume),
            str(self.last),
            str(self.open_interest),
        ])

    def __repr__(self):
        return (f'<WindMinuteBarData(ticker={self.ticker}, '
                f'datatime={str(self.datatime)},'
                f'open={self.open}, high={self.high}, low={self.low}, close={self.close}, '
                f'volume={self.volume},'
                f'open_interest={self.open_interest}')

    def __init__(
            self,
            ticker, datatime, open, high, low, close, volume, open_interest,
            **kwargs
    ):
        self.ticker = str(ticker)
        if type(datatime) is str:
            if '-' in date:
                self.datatime = datetime.strptime(datatime, "%Y-%m-%d")
            elif '/' in date:
                self.datatime = datetime.strptime(datatime, "%Y/%m/%d")
            else:
                self.datatime = datetime.strptime(datatime, "%Y%m%d")
        else:
            self.datatime = datatime
        self.open = float(open)
        self.high = float(high)
        self.low = float(low)
        self.close = float(close)
        self.last = self.close
        self.volume = float(volume)
        self.open_interest = float(open_interest)


class WindMinuteBarFile:
    @classmethod
    def to_file(cls, data: List[WindMinuteBarData], path):
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
            # f.write(','.join(list(WindMinuteBarData.__annotations__.keys())) + '\n')
            f.writelines('\n'.join([str(_) for _ in data]))

    @classmethod
    def from_file(cls, path, s_date, ticker) -> List[WindMinuteBarData]:
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
            try:
                _time = line_split[0]
                _o = line_split[1]
                _h = line_split[2]
                _l = line_split[3]
                _c = line_split[4]
                _v = line_split[5]
                _last = line_split[6]
                _oi = line_split[7]
            except Exception as e:
                raise e
            l_data.append(WindMinuteBarData(
                ticker=ticker,
                datatime=datetime.strptime(f'{s_date}_{_time}', '%Y%m%d_%H:%M:%S'),
                open=_o,
                high=_h,
                low=_l,
                close=_c,
                volume=_v,
                open_interest=_oi
            ))
        return l_data

