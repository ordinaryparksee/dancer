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


@dataclass
class IndexMeta:
    table_name: str
    non_unique: bool
    key_name: str
    seq_in_index: str
    column_name: str
    collation: str
    cardinality: str
    sub_part: str
    packed: str
    null: str
    index_type: str
    comment: str
    index_comment: str


@dataclass
class ReferenceMeta:
    table_name: str
    column_name: str
    referenced_table_name: str
    referenced_column_name: str
