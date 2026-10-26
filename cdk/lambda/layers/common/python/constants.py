from enum import StrEnum

class Status(StrEnum):
    ACTIVE = 'ACTIVE'
    INACTIVE = 'INACTIVE'
    PENDING = 'PENDING'
    FAILED = 'FAILED'
