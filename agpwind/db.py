from urllib import parse
from datetime import datetime, date

from .object import WindGeneralTickerInfoData

from sqlalchemy import Column, String, Integer, Date, Float, ForeignKey, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker


Base = declarative_base()  # 创建对象的基类


class WindGeneralTickerInfo(Base):  # 定义一个类，继承Base
    __tablename__ = 'WindGeneralTickerInfo'

    """
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
    """

    product = Column(String(256), primary_key=True)
    date = Column(Date, primary_key=True)
    ticker = Column(String(256))
    point_value = Column(Float)
    min_move = Column(Float)
    commission_on_rate = Column(Float)
    commission_per_share = Column(Float)
    flat_today_discount = Column(Float)
    margin = Column(Float)

    def __repr__(self):
        return (f'<WindGeneralTickerInfo(Product={self.product}, '
                f'Date={str(self.date)}, Ticker={self.ticker}, '
                f'PointValue={self.point_value}, MinMove={self.min_move}, '
                f'CommissionOnRate={self.commission_on_rate}, CommissionPerShare={self.commission_per_share}, '
                f'FlatTodayDiscount={self.flat_today_discount}, Margin={self.margin}')

    def __str__(self):
        return ','.join(str(_) for _ in [
            self.point_value, self.date, self.ticker, self.point_value, self.margin,
            self.commission_on_rate, self.commission_per_share, self.flat_today_discount, self.margin
        ])

    def to_dict(self):
        return {
            "product": self.product,
            "date": self.date,
            "ticker": self.ticker,
            "point_value": self.point_value,
            "min_move": self.min_move,
            "commission_on_rate": self.commission_on_rate,
            "commission_per_share": self.commission_per_share,
            "flat_today_discount": self.flat_today_discount,
            "margin": self.margin
        }

    @classmethod
    def from_inner_data(cls, data: WindGeneralTickerInfoData):
        return cls(**data.__dict__)

    def to_inner_data(self) -> WindGeneralTickerInfoData:
        return WindGeneralTickerInfoData(**self.to_dict())



