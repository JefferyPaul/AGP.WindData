"""
通过wind的 general ticker info数据，检查有“异常”的数据。

1. 从数据库获取wind最进2天的GTI数据。
2. 指定“标准的GTI数据”，指定文件。
3. 对比差异
    1) 对比wind今天的数据和昨天的数据。有变化则报警
    2) 对比wind今天的数据与“标准”数据，识别“异常“数据。

"""
import os
import shutil
import sys
from datetime import datetime
from typing import List
import json

PATH_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.append(PATH_ROOT)

from agpwind.object import WindGeneralTickerInfoFile, WindGeneralTickerInfoData
from agpwind.db import WindGeneralTickerInfo
from pyptools.helper.simpleLogger import MyLogger

import argparse

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-i', '--input', default=os.path.join(PATH_ROOT, 'StandardWindGeneralTickerInfo.csv'))
arg_parser.add_argument('-o', '--output', default=os.path.join(PATH_ROOT, 'Output', 'CheckingWindGTI'))
args = arg_parser.parse_args()
input_file = os.path.abspath(args.input)
output_root = os.path.abspath(args.output)

assert os.path.isfile(input_file)
if not os.path.isdir(output_root):
    os.makedirs(output_root)

p_config_file = os.path.join(PATH_ROOT, 'Config', 'Config.json')
assert os.path.isfile(p_config_file)


def _download_wind_gti(db_config) -> (List[WindGeneralTickerInfoData], List[WindGeneralTickerInfoData]):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import relationship, sessionmaker
    from sqlalchemy.sql import func
    from urllib import parse

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
    db_data_date = list(set([_[0] for _ in session.query(WindGeneralTickerInfo.date).all()]))
    db_data_date.sort()
    print(db_data_date)
    newest_date = db_data_date[-1]
    if len(db_data_date) >= 2:
        second_date = db_data_date[-2]
    else:
        second_date = None
    print(newest_date, second_date)

    l_db_newest_data: List[WindGeneralTickerInfo] = session.query(
        WindGeneralTickerInfo).filter(WindGeneralTickerInfo.date == newest_date).all()
    if second_date:
        l_db_second_data: List[WindGeneralTickerInfo] = session.query(
            WindGeneralTickerInfo).filter(WindGeneralTickerInfo.date == second_date).all()
    else:
        l_db_second_data = []
    return [_.to_inner_data() for _ in l_db_newest_data], [_.to_inner_data() for _ in l_db_second_data]


def check_general_ticker_info(
        standard: List[WindGeneralTickerInfoData],
        checking: List[WindGeneralTickerInfoData]
) -> List[List[WindGeneralTickerInfoData]]:
    # 是否有缺少/增加
    d_standard_data = {
        _.product: _
        for _ in standard
    }
    d_checking_data = {
        _.product: _
        for _ in checking
    }
    l_standard_product = list(d_standard_data.keys())
    l_checking_product = list(d_checking_data.keys())
    if set(l_standard_product) != set(l_checking_product):
        checking_list_missing_product = list(set(l_standard_product).difference(set(l_checking_product)))
        checking_list_exclusive_product = list(set(l_checking_product).difference(set(l_standard_product)))
        if checking_list_missing_product:
            for _ in checking_list_missing_product:
                logger.warning(f'缺少此product, {_}')
        if checking_list_exclusive_product:
            for _ in checking_list_exclusive_product:
                logger.warning(f'新增此product, {_}')

    # 对比
    _error_list = []
    for _product in l_standard_product:
        if _product not in l_checking_product:
            continue
        _standard_data: WindGeneralTickerInfoData = d_standard_data[_product]
        _checking_data: WindGeneralTickerInfoData = d_checking_data[_product]
        #
        _error = False
        if _checking_data.min_move != _standard_data.min_move:
            logger.error(f'数据有误, {_product}, min_move, {_checking_data.min_move}, {_standard_data.min_move}')
            _error = True
        #
        if _checking_data.point_value != _standard_data.point_value:
            logger.error(f'数据有误, {_product}, point_value, {_checking_data.point_value}, {_standard_data.point_value}')
            _error = True
        #
        if _checking_data.margin != _standard_data.margin:
            logger.error(f'数据有误, {_product}, margin, {_checking_data.margin}, {_standard_data.margin}')
            _error = True
        #
        if _checking_data.commission_per_share != _standard_data.commission_per_share:
            logger.error(f'数据有误, {_product}, commission_per_share,'
                         f' {_checking_data.commission_per_share}, {_standard_data.commission_per_share}')
            _error = True
        #
        if _checking_data.commission_on_rate != _standard_data.commission_on_rate:
            logger.error(f'数据有误, {_product}, commission_on_rate, '
                         f'{_checking_data.commission_on_rate}, {_standard_data.commission_on_rate}')
            _error = True
        #
        if _checking_data.flat_today_discount != _standard_data.flat_today_discount:
            logger.error(f'数据有误, {_product}, flat_today_discount,'
                         f' {_checking_data.flat_today_discount}, {_standard_data.flat_today_discount}')
            _error = True
        if _error:
            _error_list.append([_checking_data, _standard_data])
    return _error_list


if __name__ == '__main__':
    logger = MyLogger('CheckGeneralTickerInfo', output_root=os.path.join(PATH_ROOT, 'logs'))
    # 从db获取最新数据
    p_config = os.path.join(PATH_ROOT, 'Config', 'Config.json')
    assert os.path.isfile(p_config)
    d_config = json.loads(open(p_config, encoding='utf-8').read())
    _db_config = d_config.get('db')
    l_newest_data, l_second_data = _download_wind_gti(_db_config)

    # 读取标准数据
    l_standard_gti_data: List[WindGeneralTickerInfoData] = WindGeneralTickerInfoFile.from_file(input_file)

    # 比较差异
    # 1 最新变化
    newest_error: List[List[WindGeneralTickerInfoData]] = check_general_ticker_info(
        standard=l_second_data, checking=l_newest_data)
    if newest_error:
        logger.warning('最新变化:')
        for _ in newest_error:
            logger.warning(f'最新变化, {str(_[0])}, {str(_[1])}')
    p_output_file_changed = os.path.join(output_root, 'gti_newest_changed.csv')
    p_output_file_changed_bak = os.path.join(
        output_root, 'gti_newest_changed_%s.csv' % datetime.now().strftime('%Y%m%d_%H%M%S'))
    newest_error_all = []
    for _ in newest_error:
        newest_error_all += _
    WindGeneralTickerInfoFile.to_file(newest_error_all, p_output_file_changed)
    shutil.copyfile(p_output_file_changed, p_output_file_changed_bak)

    # 2 异常数据
    error_list: List[List[WindGeneralTickerInfoData]] = check_general_ticker_info(
        standard=l_standard_gti_data, checking=l_newest_data)
    if error_list:
        logger.warning('非标准设定:')
        for _ in error_list:
            logger.warning(f'非标准设定, {str(_[0])}, {str(_[1])}')
