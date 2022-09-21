from __future__ import annotations

from typing import List


def filter_in(a: list, b: list) -> list:
    return list(filter(lambda _a: _a in b, a))


def filter_not_in(a: list, b: list) -> list:
    return list(filter(lambda _a: _a not in b, a))


def map_format(a: List[str], format: str) -> list:
    return list(map(lambda _a: format.format(_a), a))


def debug_query(sql: str, **kwargs):
    for key in list(reversed(sorted(kwargs.keys()))):
        value = kwargs[key]
        sql = sql.replace(':' + key, f"'{value}'")
    print(sql)
