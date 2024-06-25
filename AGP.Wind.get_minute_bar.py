import os
import sys
from datetime import datetime
from pprint import pprint
from typing import List


PATH_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.append(PATH_ROOT)

from agpwind.method import get_wind_minute_bar, start_wind, close_wind, _inner_symbol_to_wind, output_wind_minute_bar_data
from agpwind.object import WindMinuteBarData
from helper.mylogger import setup_logging
import logging

setup_logging()
logger = logging.getLogger('APG.Wind.get_daily_bar')

import argparse

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--symbol', default='')
arg_parser.add_argument('-s', '--start', default='')
arg_parser.add_argument('-e', '--end', default='')
arg_parser.add_argument('-i', '--input', default='', help='输入文件, symbols')
arg_parser.add_argument('-o', '--output', default='')
args = arg_parser.parse_args()
symbol = args.symbol
start_date = args.start
end_date = args.end
input_file = args.input
output_root = args.output

if symbol:
    input_file = ''
if start_date:
    start_date = datetime.strptime(start_date, '%Y%m%d').date()
if end_date:
    end_date = datetime.strptime(end_date, '%Y%m%d').date()
if input_file:
    input_file = os.path.abspath(input_file)
    assert os.path.isfile(input_file)
if output_root:
    if not os.path.isdir(output_root):
        os.makedirs(output_root)


if __name__ == '__main__':
    l_symbols_start_end = list()

    # 读取 输入文件
    if input_file:
        with open(input_file, 'r', encoding='utf-8') as f:
            l_lines = f.readlines()
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            line_split = line.split(',')
            _symbol = line_split[0]
            _start = datetime.strptime(line_split[1], '%Y%m%d').date()
            _end = datetime.strptime(line_split[2], '%Y%m%d').date()
            l_symbols_start_end.append([_symbol, _start, _end])
    else:
        l_symbols_start_end.append([symbol, start_date, end_date])

    # 获取
    start_wind()
    l_all_data: List[WindMinuteBarData] = []
    for _symbol, _start, _end in l_symbols_start_end:
        _wind_symbol = _inner_symbol_to_wind(_symbol)
        l_data: List[WindMinuteBarData] = get_wind_minute_bar(
            symbol=_wind_symbol, start_date=_start, end_date=_end)
        # pprint(l_data, indent=4)
        l_all_data += l_data
    close_wind()

    # WindDailyBarFile.to_file(l_all_data, output_file)
    # if output_file:
    #     WindDailyBarFile.to_file(l_all_data, output_file)
    if output_root:
        output_wind_minute_bar_data(data=l_all_data, output_root=output_root)
