# AGP.Platinum.DS

Platinum.DS的python版本。主要目的是方便外部调用和处理DS的数据，暂时不涉及数据接收，所以主要实现一些外部常用的方法。
另外，为了更好地模仿DS，所以也会尽可能地完整地实现DS的功能。

## 几个原则

- 尽可能使用原DS的配置。此工具只需要指定原DS的路径，就可以使用。
- 尽可能模仿DS的功能和功能实现基础。包括 Bar / ticker info / trading session / holiday / back adjust / product - ticker， 等等。
- 提供常用方法的外部调用入口。
- 后期根据需要再考虑添加 s-c模式。

## pyptools.common

### pyptools.common.general_ticker_info

- TickerInfoData
  数据类型,存储数据.  
    product: Product
    prefix: str  # Futures / Stock / Options / ...
    currency: str
    point_value: float  # 1张合约 的 “价值 / 报价”乘数，   value / share = price * point_value
    min_move: float  # 价格最小变动幅度；1个tick 对应的 价格变动数值
    lot_size: float  # 最少交易多少手 （的倍数），1手 是多少 张(shares)
    commission_on_rate: float  # 手续费，交易价值的比率
    commission_per_share: float  # 手续费，每张多少钱
    slippage_points: float
    flat_today_discount: float  # 平今佣金倍率。1：相同；0：不收钱；2：收2倍
    margin: float  # 保证金率
- GeneralTickerInfoFile   
  .read() 文件读取, -> Dict[Product, TickerInfoData]
- GeneralTickerInfoManager  
  输入目录, 如 "./Platinum/Platinum.Ds/Release/Data", 查找该目录下的 文件夹, 文件夹名作为 time zone index  
  作为在Platinum组件中使用的用于管理GeneralTickerInfo的工具

### pyptools.common.trading_session
