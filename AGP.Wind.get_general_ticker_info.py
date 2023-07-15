import os
import sys
from datetime import datetime
from pprint import pprint
from typing import List


PATH_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.append(PATH_ROOT)

from agpwind.method import get_wind_general_ticker_info, start_wind, end_wind, inner_symbol_to_wind
from agpwind.object import WindGeneralTickerInfoData, WindGeneralTickerInfoFile

import argparse

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--symbol', default='')
arg_parser.add_argument('-s', '--start', default='')
arg_parser.add_argument('-e', '--end', default='')
arg_parser.add_argument('-i', '--input', default='', help='输入文件')
arg_parser.add_argument('-o', '--output', default='')
args = arg_parser.parse_args()
symbol = args.symbol
start_date = args.start
end_date = args.end
input_file = args.input
output_file = args.output

if symbol:
    input_file = ''
if start_date:
    start_date = datetime.strptime(start_date, '%Y%m%d').date()
if end_date:
    end_date = datetime.strptime(end_date, '%Y%m%d').date()
if input_file:
    input_file = os.path.abspath(input_file)
    assert os.path.isfile(input_file)
if output_file:
    output_file = os.path.abspath(output_file)
    if not os.path.isdir(os.path.dirname(output_file)):
        os.makedirs(os.path.dirname(output_file))


if __name__ == '__main__':
    # 读取 输入文件
    if input_file:
        with open(input_file, 'r', encoding='utf-8') as f:
            l_lines = f.readlines()
        l_symbols = []
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            l_symbols.append(line)
    else:
        l_symbols = [symbol]

    # 获取
    start_wind()
    l_all_data: List[WindGeneralTickerInfoData] = []
    for symbol in l_symbols:
        l_data: List[WindGeneralTickerInfoData] = get_wind_general_ticker_info(
            symbol=inner_symbol_to_wind(symbol), start_date=start_date, end_date=end_date)
        pprint(l_data, indent=4)
        l_all_data += l_data
    end_wind()

    if output_file:
        WindGeneralTickerInfoFile.to_file(l_all_data, output_file)
