import os
import shutil
import sys
from datetime import datetime, timedelta
from time import sleep

PATH_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.append(PATH_ROOT)

import pandas as pd
import numpy as np

from helper.mylogger import setup_logging
import logging

setup_logging()
logger = logging.getLogger('APG.Wind.get_fund_net_value')

try:
    from WindPy import w
except:
    logger.error('import WindPy error')
    raise ImportError

import argparse

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-s', '--start', default='')
arg_parser.add_argument('-e', '--end', default='')
arg_parser.add_argument('-i', '--input', default='', help='输入文件')
arg_parser.add_argument('-o', '--output', default='')
args = arg_parser.parse_args()
start_date = args.start
end_date = args.end
input_file = args.input
output_root = args.output

if end_date:
    end_date = datetime.strptime(end_date, '%Y%m%d').date()
else:
    end_date = datetime.today().date()
if start_date:
    start_date = datetime.strptime(start_date, '%Y%m%d').date()
else:
    start_date = end_date - timedelta(days=30)

input_file = os.path.abspath(input_file)
assert os.path.isfile(input_file)

if os.path.isdir(output_root):
    shutil.rmtree(output_root)
    sleep(0.1)
os.makedirs(output_root)
# path_output_nv_data = output_root
# if not os.path.isdir(path_output_nv_data):
#     os.makedirs(path_output_nv_data)
# path_output_aps = os.path.join(output_root, 'APS')
# if not os.path.isdir(path_output_aps):
#     os.makedirs(path_output_aps)


if __name__ == '__main__':
    # 读取 输入文件
    l_fund_info = list()
    with open(input_file, 'r', encoding='utf-8') as f:
        l_lines = f.readlines()
    for line in l_lines:
        line = line.strip()
        if line == '':
            continue
        l_fund_info.append([line.split(',')[0], line.split(',')[1]])

    # 获取
    w.start()

    # l_all_data = []
    l_fund_df = list()
    l_fund_name = list()
    for _id, _name in l_fund_info:
        print(_name)
        # 从 wind 获取数据
        wsd_data = w.wsd(
            _id, "NAV_adj",
            start_date,
            end_date,
            "Period=W"
        )
        if wsd_data.ErrorCode == 0:
            pass
        else:
            logger.error(f"Error Code: {wsd_data.ErrorCode}")
            logger.error(f"Error Message: {wsd_data.Data[0][0]}")
            os.system('pause')

        # 解析数据
        l_fund_data = list()
        for n in range(len(wsd_data.Times)):
            _date = wsd_data.Times[n]
            _nv = wsd_data.Data[0][n]
            l_fund_data.append({
                "Date": _date,
                "NetValue": _nv
            })
        # 得到 DataFrame 数据
        df = pd.DataFrame(l_fund_data)

        # 数据清晰
        df['Date'] = df['Date'].apply(lambda x: x.strftime('%Y%m%d'))
        df.set_index(keys=['Date'], inplace=True)
        df.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
        df.dropna(inplace=True)
        if len(df) < 3:
            logger.info(f'数据量不足, {_name}')
            continue
        df.to_csv(
            os.path.join(output_root, _name + '.csv'),
            header=False
            # index=False
        )

        # df_2 = df.fillna(method='bfill').fillna(0)
        # if len(df_2) < 3:
        #     logger.info(f'数据量不足, {_name}')
        #     continue
        l_fund_df.append(df)
        l_fund_name.append(_name)

        # df_diff = df_2.diff().iloc[1:]
        # _p_aps_folder = os.path.join(path_output_aps, _name)
        # if not os.path.isdir(_p_aps_folder):
        #     os.makedirs(_p_aps_folder)
        # df_diff.to_csv(
        #     os.path.join(_p_aps_folder, 'AggregatedPnlSeries.csv'),
        #     header=None
        #     # index=False
        # )

    # 汇总
    df_all = pd.concat(
        l_fund_df, axis=1,
       # keys=l_fund_name
   )
    df_all.fillna(method='bfill', inplace=True)
    df_all.columns = l_fund_name
    df_all.to_csv(os.path.join(output_root, '_all.csv'))

    w.close()
