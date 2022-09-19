from dataclasses import dataclass


@dataclass
class ColumnMeta:
    field: str
    type: str
    collation: str
    null: str
    key: str
    default: str
    extra: str
    privileges: str
    comment: str
