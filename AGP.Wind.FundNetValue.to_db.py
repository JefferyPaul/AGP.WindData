# encoding:utf-8

"""
只插入新数据。重复的数据不会覆盖旧数据。
"""

# 内置库
from datetime import datetime, timedelta
import json
import os
import sys
import logging
import argparse

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
logger = logging.getLogger('update_nv_data_to_db')

from agpwind.db import WindNetValues, creating_db_session

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-c', '--dbinfo', default=os.path.join(PATH_PROJECT, 'Config', 'DBInfo.json'))
arg_parser.add_argument('-i', '--input')
arg_parser.add_argument('--days', default=15)
args = arg_parser.parse_args()
PATH_DB_INFO_FILE = os.path.abspath(args.dbinfo)
PATH_INPUT = os.path.abspath(args.input)
CHECKING_DAYS = int(args.days)
assert os.path.isfile(PATH_DB_INFO_FILE)
assert os.path.isdir(PATH_INPUT)


def wind_nv_data_2_db(db_session: Session, date, fund, net_value):
    nv_obj = WindNetValues(date=date, fund=fund, net_value=net_value)
    db_session.add(nv_obj)
    try:
        db_session.commit()
    except Exception as e:
        db_session.rollback()
        result = db_session.query(WindNetValues.Date, WindNetValues.Fund).filter(
            WindNetValues.Date == nv_obj.Date, WindNetValues.Fund == nv_obj.Fund
        ).all()
        if result:
            pass
        else:
            print(e)
            logger.error(e)
            logger.warning('插入失败: %s ' % str(nv_obj))
    else:
        logger.info('插入新数据: %s' % str(nv_obj))
        pass

def main():
    # 【0】  读取config
    db_info: dict = json.loads(
        open(PATH_DB_INFO_FILE, encoding='utf-8').read(),
        encoding='utf-8'
    )
    host = db_info['host']
    database = db_info['database']
    user = db_info['user']
    pwd = db_info['pwd']

    # 输入到数据库
    db_session = creating_db_session(host=host, user=user, pwd=pwd, database=database)

    # 读取数据
    dt_today = datetime.today().date()
    start_date_to_input = (dt_today - timedelta(days=CHECKING_DAYS)).strftime('%Y%m%d')
    for _file_name in os.listdir(PATH_INPUT):
        p_file = os.path.join(PATH_INPUT, _file_name)
        if not os.path.isfile(p_file):
            continue
        if _file_name == '_all.csv':
            continue
        if _file_name.split('.')[-1] != 'csv':
            continue
        _fund_name = _file_name[:-4]

        with open(p_file, encoding='utf-8') as f:
            l_lines = f.readlines()
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            if line.find('Date') == 0:
                continue
            _date = line.split(',')[0]
            _net_value = line.split(',')[1]
            if _date < start_date_to_input:
                continue
            else:
                wind_nv_data_2_db(
                    db_session=db_session,
                    fund=_fund_name,
                    date=_date,
                    net_value=_net_value
                )

    # 关闭
    db_session.close()


if __name__ == '__main__':
    logger.info('Start')
    main()
    logger.info('Finished')

