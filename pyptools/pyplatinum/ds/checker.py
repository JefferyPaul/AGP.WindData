"""

数据检查器

"""


class BarDataChecker:
    def __init__(self):
        pass


class DSChecker:
    """
        1) Bar数据检查,
            分钟数据是否有缺失,OHLCV检查;
                MostActivate
                TradingSession
        2) Tick数据盘中监控,
    """

    def __init__(self, root, logger=logging.Logger('DSChecker')):
        self.ds_manager = DSManager(root, logger=logger)
        self.logger = logger

    def _check_bar_data(self, data: List[BarData]):
        for _a_bar in data:
            pass

    def get_most_activate_ticker_in_exchange(self, tdate: date, products: List[Product]):
        for _product in products:
            # 获取 最活跃合约
            _mat: Ticker = self.ds_manager.get_a_most_activate_ticker(_product, tdate)
            if not _mat:
                self.logger.warning(f'找不到此Product的MostActivateTicker, {_product.name}, {tdate.strftime("%Y%m%d")}')
                continue

    def check_ticker_bar(self, tdate: date, tickers: List[Ticker]):
        _default_timezone = '210'
        _trading_session_table = self.ds_manager.trading_session_manager
        for _ticker in tickers:
            # 读取 bar数据
            _bar_data: List[BarData] = self.ds_manager._get_ticker_bar(ticker=_ticker, query_date=tdate)
            if not _bar_data:
                self.logger.warning(f'找不到此Ticker的Bar数据, {_ticker.name}, {tdate.strftime("%Y%m%d")}')
                continue
            # 检查数据
            # 获取trading session
            _ticker_trading_session = _trading_session_table.get(_default_timezone, _ticker.product, tdate)
            # check1 连续

    @staticmethod
    def _check_trading_session(
            data: List[BarData], trading_session_data: TradingSessionData) -> List[time]:
        _data_times: List[time] = [_.time for _ in data]        # 所有bar 时间
        _data_times.sort()
        _checking_time_n = -1       # 用于遍历 需要检查的 交易时间
        _data_time_n = 0            # 用于遍历 所有bar的 数据时间
        l_losing_time = []      # 存储缺少的时间
        # 根据trading session 指定的时间区间进行检查
        for _a_session in trading_session_data.TradingSession:
            _session_start: time = _a_session[0]
            _session_end: time = _a_session[1]
            while True:
                # 需要检查的 交易时间
                _checking_time_n += 1
                _checking_time = AllMinuteTime[_checking_time_n]
                if _checking_time < _session_start:
                    continue
                if _checking_time > _session_end:
                    break
                # 看看bar数据中有没有此时间
                while True:
                    _data_time = _data_times[_data_time_n]
                    if _data_time < _checking_time:
                        _data_time_n += 1
                        continue
                    elif _data_time == _checking_time:
                        # 存在此时间的数据
                        break
                    else:
                        # 不存在此时间的数据
                        l_losing_time.append(_checking_time)
                        break
        return l_losing_time

