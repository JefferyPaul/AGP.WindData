import os
import shutil
import sys
from datetime import datetime
from pprint import pprint
from typing import List
import json

PATH_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.append(PATH_ROOT)

from agpwind.method import get_wind_general_ticker_info, start_wind, close_wind, _inner_symbol_to_wind
from agpwind.object import WindGeneralTickerInfoData, WindGeneralTickerInfoFile
from agpwind.db import WindGeneralTickerInfo
from helper.mylogger import setup_logging
import logging

setup_logging()
logger = logging.getLogger('APG.Wind.get_general_ticker_info')

import argparse

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--symbol', default='')             # 查询单个symbol
arg_parser.add_argument('-i', '--input', default='', help='输入文件')       # 读取文件，查询文件中的所有symbol
arg_parser.add_argument('-o', '--output', default=os.path.join(PATH_ROOT, 'Output', 'WindGeneralTickerInfo'))
arg_parser.add_argument('-s', '--start', default='')
arg_parser.add_argument('-e', '--end', default='')
arg_parser.add_argument('--db', action='store_true')
args = arg_parser.parse_args()
symbol = args.symbol
start_date = args.start
end_date = args.end
input_file = args.input
path_output_root = os.path.abspath(args.output)
is_saving_db = args.db

if symbol:
    input_file = ''
if start_date:
    start_date = datetime.strptime(start_date, '%Y%m%d').date()
else:
    start_date = datetime.today().date()
if end_date:
    end_date = datetime.strptime(end_date, '%Y%m%d').date()
else:
    end_date = datetime.today().date()
if input_file:
    input_file = os.path.abspath(input_file)
    assert os.path.isfile(input_file)
if not os.path.isdir(path_output_root):
    os.makedirs(path_output_root)


def _to_wind_gti_db(db_config, data: List[WindGeneralTickerInfoData]):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from urllib import parse

    logger.info(f'wind gti data to db')

    engine = create_engine(
        f'mssql+pymssql://{str(db_config["user"])}:{parse.quote_plus(db_config["pwd"])}@'
        f'{str(db_config["host"])}/{str(db_config["db"])}',
        echo=False,
        max_overflow=50,  # 超过连接池大小之后，允许最大扩展连接数；
        pool_size=50,  # 连接池的大小
        pool_timeout=600,  # 连接池如果没有连接了，最长的等待时间
        pool_recycle=-1,  # 多久之后对连接池中连接进行一次回收
    )

    # 创建DBSession类
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    for _data in data:
        session.add(WindGeneralTickerInfo.from_inner_data(_data))
        try:
            session.commit()
        except :
            logger.warning(f'插入db失败, {_data.product}, {str(_data.date)}')


if __name__ == '__main__':
    logger.info('start')

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
        logger.info(f'checking {symbol}, {str(start_date)}, {str(end_date)}')
        l_data: List[WindGeneralTickerInfoData] = get_wind_general_ticker_info(
            symbol=_inner_symbol_to_wind(symbol),
            start_date=start_date,
            end_date=end_date
        )
        l_all_data += l_data
    close_wind()

    logger.info('output file')
    output_file = os.path.join(path_output_root, 'TickerInfos.csv')
    output_file_bak = os.path.join(path_output_root, 'TickerInfos_%s.csv' % datetime.now().strftime('%Y%m%d_%H%M%S'))
    WindGeneralTickerInfoFile.to_file(l_all_data, output_file)
    shutil.copyfile(output_file, output_file_bak)

    if is_saving_db:
        # 检查数据是否完备，若否，则不插入db
        _error = False
        l_all_data_checked = []
        for _data in l_all_data:
            if not _data.check():
                logger.error(f'不完整的数据, {str(_data)}')
                _error = True
            else:
                l_all_data_checked.append(_data)

        logger.info('to db')
        p_config = os.path.join(PATH_ROOT, 'Config', 'Config.json')
        assert os.path.isfile(p_config)
        d_config = json.loads(open(p_config, encoding='utf-8').read())
        _db_config = d_config.get('db')
        _to_wind_gti_db(_db_config, l_all_data_checked)
        if _error:
            pass


