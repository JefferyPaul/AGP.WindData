import os
from datetime import datetime, date
from typing import List, Dict
from collections import defaultdict



"""
/platinum/Release/Data/Holiday

提供方法：输入日期，判断是否节假日
不考虑trading session。简单地判断是否节假日（是否交易日）
"""


class HolidayFile:
    FileName = 'Holiday.csv'

    @classmethod
    def read(cls, p) -> list:
        assert os.path.isfile(p)
        with open(p) as f:
            l_lines = f.readlines()
        l_data = []
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            line_split = line.split(',')
            assert len(line_split) == 2
            _exchange = line_split[0]
            _holiday = datetime.strptime(line_split[1], '%Y/%m/%d').date()
            l_data.append((_exchange, _holiday))
        return l_data


class HolidayManager:
    """
    指定 holiday.csv 文件，读取。
    提供一些常用方法，如：
        返回所有 holiday 信息
        判断某个日期是否holiday
    """
    def __init__(self, path):
        self._file_path = path
        self._holiday: list = HolidayFile.read(path)
        self._holiday_by_exchange: Dict[str, List[date]] = defaultdict(list)
        for _exchange, _holiday in self._holiday:
            self._holiday_by_exchange[_exchange].append(_holiday)
        self._all_holiday: List[date] = list(set([_[1] for _ in self._holiday]))

    @property
    def holiday(self):
        return self._holiday.copy()

    @property
    def holiday_by_exchange(self):
        return self._holiday_by_exchange

    @property
    def all_holiday(self):
        return self._all_holiday

    def is_holiday(self, checking_date: date or datetime, exchange: str = None) -> bool:
        if type(checking_date) is datetime:
            checking_date = checking_date.date()
        elif type(checking_date) is date:
            pass
        else:
            raise TypeError

        _holiday = []
        if exchange:
            if exchange in self._holiday_by_exchange.keys():
                _holiday = self._holiday_by_exchange[exchange]
            else:
                raise KeyError
        else:
            _holiday = self.all_holiday
        return checking_date in _holiday
