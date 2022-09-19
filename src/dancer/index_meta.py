from dataclasses import dataclass


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
