from enum import StrEnum

class Status(StrEnum):
    ACTIVE = 'ACTIVE'
    INACTIVE = 'INACTIVE'
    PENDING = 'PENDING'
    FAILED = 'FAILED'

class SchemaType(StrEnum):
    MBO = "mbo"
    MBP_10 = "mbp-10"
    MBP_1 = "mbp-1"
    BBO_1S = "bbo-1s"
    BBO_1M = "bbo-1m"
    TRADES = "trades"
    OHLCV_1S = "ohlcv-1s"
    OHLCV_1M = "ohlcv-1m"
    OHLCV_1H = "ohlcv-1h"
