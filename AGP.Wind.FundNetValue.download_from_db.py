# encoding:utf-8

"""

"""
import shutil
# 内置库
from time import sleep
from datetime import datetime
import json
import os
import sys
import logging
from typing import List, Dict
import pandas as pd
import argparse
from collections import defaultdict

# 第三方库
from sqlalchemy.orm import Session

# 调用项目内部库，
# 需要将项目路径加入 sys.path 中
PATH_PROJECT = os.path.dirname(os.path.abspath(__file__))
os.chdir(PATH_PROJECT)
sys.path.append(PATH_PROJECT)

# 本地库
from helper.mylogger import setup_logging
setup_logging()
logger = logging.getLogger('get_nv_data_from_db_in_fund')

from agpwind.db import WindNetValues, creating_db_session

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-c', '--dbinfo', default=os.path.join(PATH_PROJECT, 'Config', 'DBInfo.json'))
arg_parser.add_argument('-o', '--output', default=os.path.join(PATH_PROJECT, 'Output_FundNetValues_FromDB'))
arg_parser.add_argument('--start', default='20231001')
args = arg_parser.parse_args()
PATH_DB_INFO_FILE = os.path.abspath(args.dbinfo)
PATH_OUTPUT_ROOT = os.path.abspath(args.output)
START_DATE = str(args.start)
assert os.path.isfile(PATH_DB_INFO_FILE)
if os.path.isdir(PATH_OUTPUT_ROOT):
    shutil.rmtree(PATH_OUTPUT_ROOT)
    sleep(0.1)
os.makedirs(PATH_OUTPUT_ROOT)


if __name__ == '__main__':
    logger.info('Start')

    # 连接数据库
    db_info: dict = json.loads(
        open(PATH_DB_INFO_FILE, encoding='utf-8').read(),
        encoding='utf-8'
    )
    host = db_info['host']
    database = db_info['database']
    user = db_info['user']
    pwd = db_info['pwd']
    db_session = creating_db_session(host=host, user=user, pwd=pwd, database=database)

    # 从数据库获取
    result = db_session.query(WindNetValues.Date, WindNetValues.Fund, WindNetValues.NetValue).filter(
        WindNetValues.Date >= START_DATE
    ).all()
    db_session.close()
    # print(result)

    # 转化数据格式
    d_all_data_gb_fund = defaultdict(list)
    for _data in result:
        # _date = datetime.strptime(_data[0], '%Y%m%d')
        # if (_date.weekday() == 6) or (_date.weekday() == 5):
        #     # 防止误入了周六周日的净值
        #     continue
        d_all_data_gb_fund[_data[1]].append({
            "Date": _data[0],
            "Fund": _data[1],
            "NetValue": _data[2],
        })

    for _fund, _data in d_all_data_gb_fund.items():
        df = pd.DataFrame(_data)
        df.sort_values('Date', inplace=True)
        # output_1 净值
        path_output_csv = os.path.join(PATH_OUTPUT_ROOT, _fund + '.csv')
        df.to_csv(path_output_csv, columns=['Date', 'NetValue'], index=False, header=False, encoding='utf-8')

    logger.info('Finished')

