from __future__ import annotations

from typing import List, Tuple, Optional, Dict, Any, Union
import random
from dataclasses import dataclass, field
from enum import Enum
from sqlalchemy import text
from sqlalchemy.engine import Connection, CursorResult, Row
from rich.progress import (
    Progress, TextColumn, BarColumn, MofNCompleteColumn, TimeElapsedColumn,
    TimeRemainingColumn
)
from .meta import ColumnMeta, IndexMeta, ReferenceMeta


class ColumnNotFoundException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class Column:
    class Reference(Enum):
        NO = 0
        TO = 1
        FROM = 2

    def __init__(self, table: Table, meta: ColumnMeta) -> None:
        self.table = table
        self.name = meta.field
        self.canonical_name = f'{self.table.canonical_name}.`{self.name}`'
        self.type = None
        self.size = None
        self.nullable = None
        self.unsigned = None
        self.primary = None
        self.default = None
        self.on_update = None
        self.auto_increment = None
        self.references_from: List[Tuple[str, str]] = []  # 본 컬럼이 참조하는 테이블명, 필드명
        self.referenced_to: Optional[Tuple[str, str]] = None  # 본 컬럼이 참조하는 테이블명, 필드명

        _type = meta.type.strip()
        type_segments = _type.rsplit(' ', 1)

        if len(type_segments) > 1:
            _type, unsigned = type_segments
            if unsigned.lower() == 'unsigned':
                self.unsigned = True

        _type = _type.strip()

        if _type.endswith(')'):
            _type, size = _type[:-1].split('(', 1)
            self.type = _type.strip().lower()
            self.size = eval('[' + size + ']')
            if self.type not in ['enum', 'set'] and len(self.size) == 1:
                self.size = self.size[0]
        else:
            self.type = _type.lower()

        self.auto_increment = ('auto_increment' in meta.extra)
        self.nullable = (meta.null == 'YES')
        self.default = meta.default
        self.on_update = meta.extra.replace('on update ', '')

        for reference in self.table.fetch_references_from(self.name):

            self.references_from.append((
                reference.table_name, reference.column_name
            ))

        reference = self.table.fetch_referenced_to(self.name)
        if reference is not None:
            self.referenced_to = (
                reference.referenced_table_name,
                reference.referenced_column_name
            )

    def fillable(self) -> bool:
        return not self.auto_increment and 'CURRENT_TIMESTAMP' not in [
            self.default, self.on_update
        ]

    def describe(self) -> None:
        print(f" * Name: {self.name}, Type: {self.type}, Size: {self.size}, Unsigned: {self.unsigned}"
              + (f"\n   &references_from {self.references_from}" if len(self.references_from) > 0 else '')
              + (f"\n   &referenced_to {self.referenced_to}" if self.referenced_to is not None else ''))

    """ 전달된 컬럼과의 참조관계 반환
    0 - 참조없음
    1 - 전달된 컬럼을 참조
    2 - 전달된 컬럼에서 참조
    """
    def referenced_with(self, column: Column) -> Column.Reference:
        ref = self.referenced_to

        if ref is None:
            return Column.Reference.NO

        if ref[0] == column.table.name and ref[1] == column.name:
            return Column.Reference.TO

        for ref in self.references_from:
            if ref[0] == column.table.name and ref[1] == column.name:
                return Column.Reference.FROM

        return Column.Reference.NO

    def is_referenced_to(self, column: Column) -> bool:
        return self.referenced_with(column) == Column.Reference.TO

    def is_referenced_from(self, column: Column) -> bool:
        return self.referenced_with(column) == Column.Reference.FROM


class Table:
    def __init__(self, database: Database, name: str) -> None:
        self.database = database
        self.name = name
        self.canonical_name = f'{self.database.canonical_name}.`{self.name}`'
        self.columns: List[Column] = []
        self.unique_indexes: Dict[str, List[Column]] = {}
        self._column_seek = 0

        for column_meta in self.fetch_columns():
            column = Column(self, column_meta)
            self.columns.append(column)
            index_meta = self.fetch_index(column.name)
            if index_meta is not None:
                if index_meta.key_name == 'PRIMARY':
                    self.primary = True
                    continue  # primary의 unique는 일반 unique와는 다르게 취급

                if not index_meta.non_unique:
                    if index_meta.key_name not in self.unique_indexes:
                        self.unique_indexes[index_meta.key_name] = []

                    self.unique_indexes[index_meta.key_name].append(column)

    def fetch_columns(self) -> List[ColumnMeta]:
        sql = f"SHOW FULL COLUMNS FROM `{self.database.name}`.`{self.name}`"
        columns = []
        for row in self.database.query(sql):
            columns.append(ColumnMeta(*row))
        return columns

    def fetch_indexes(
            self, column_name: Optional[str] = None) -> List[IndexMeta]:
        sql = f"SHOW INDEX FROM `{self.database.name}`.`{self.name}`"
        params = {}

        if column_name is not None:
            sql += f" WHERE `Column_name` = :column"
            params['column'] = column_name

        indexes = []
        for row in self.database.query(sql, **params):
            indexes.append(IndexMeta(*row))
        return indexes

    def fetch_index(self, column_name: str) -> Optional[IndexMeta]:
        indexes = self.fetch_indexes(column_name)
        if len(indexes) > 0:
            return indexes[0]
        else:
            return None

    def fetch_references_from(
            self, column_name: Optional[str] = None) -> List[ReferenceMeta]:
        sql = f"""
            SELECT
                TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME
            FROM information_schema.key_column_usage
            WHERE REFERENCED_TABLE_SCHEMA = :database
            AND REFERENCED_TABLE_NAME = :table
        """
        params = {
            'database': self.database.name,
            'table': self.name
        }

        if column_name is not None:
            sql += f" AND `REFERENCED_COLUMN_NAME` = :column"
            params['column'] = column_name

        rows = self.database.query(sql, **params).fetchall()

        references = []
        for row in rows:
            references.append(ReferenceMeta(*row))
        return references

    def fetch_referenced_to(
            self, column_name: Optional[str] = None) -> Optional[ReferenceMeta]:
        sql = f"""
            SELECT
                TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME
            FROM information_schema.key_column_usage
            WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table
            AND REFERENCED_TABLE_NAME IS NOT NULL
            AND REFERENCED_COLUMN_NAME IS NOT NULL
        """
        params = {
            'database': self.database.name,
            'table': self.name
        }

        if column_name is not None:
            sql += f" AND `COLUMN_NAME` = :column"
            params['column'] = column_name

        row = self.database.query(sql, **params).fetchone()

        if row is None:
            return None
        else:
            return ReferenceMeta(*row)

    def fields(self) -> List[str]:
        return list(map(lambda column: column.name, self.columns))

    def fillable_fields(self) -> List[str]:
        return list(map(
            lambda column: column.name, list(filter(
                lambda column: column.fillable(), self.columns
            ))
        ))

    """ 본 테이블을 참조하는 컬럼 리스트 반환
    """

    def references_from(self) -> List[Column]:
        references = []
        for column in self.columns:
            for reference in column.references_from:
                ref_table_name, ref_column_name = reference
                ref_column = self.database[ref_table_name][ref_column_name]
                references.append(ref_column)
        return references

    """ 본 테이블이 참조하는 컬럼 리스트 반환
    """

    def references_to(self, columns: List[Column] = None) -> List[Column]:
        references = []
        if columns is None:
            columns = self.columns

        for column in columns:
            if column.referenced_to is not None:
                ref_table_name, ref_column_name = column.referenced_to
                ref_column = self.database[ref_table_name][ref_column_name]
                references.append(ref_column)
        return references

    def references_group_by_table(self) -> Dict[str, List[Column]]:
        referenced_tables = {}
        for ref_column in self.references_to():
            ref_table_name = ref_column.table.name
            if ref_table_name not in referenced_tables:
                referenced_tables[ref_table_name] = []

            referenced_tables[ref_table_name].append(ref_column)
        return referenced_tables

    def nullable_columns(self) -> List[Column]:
        columns = []
        for column in self:
            if column.nullable:
                columns.append(column)
        return columns

    def required_columns(self) -> List[Column]:
        columns = []
        for column in self:
            if not column.nullable:
                columns.append(column)
        return columns

    def fillable_columns(self) -> List[Column]:
        columns = []
        for column in self:
            if column.fillable():
                columns.append(column)
        return columns

    def is_fillable(self, column_name: str) -> bool:
        for column in self:
            if column.name == column_name:
                return True
        return False

    def get_unique_constraints(self) -> TableUniqueConstraints:
        return TableUniqueConstraints(self, self.unique_indexes)

    def fetch_random_row(
            self, table_name: str,
            seed: Optional[int] = None) -> Optional[Any]:

        row = self.database.query(f"""
            SELECT *
            FROM `{self.database.name}`.`{table_name}`
            ORDER BY RAND({seed})
        """).fetchone()

        if row is None:
            return None
        else:
            return row

    def describe(self) -> None:
        print(
                f"{self.name}\n" +
                ("-" * len(self.name)) + "\n" +
                "\n".join([str(column) for column in self])
        )

    def __str__(self):
        return f'`{self.database.name}`.`{self.name}`'

    def __iter__(self):
        self._column_seek = 0
        return self

    def __next__(self) -> Column:
        if self._column_seek < len(self.columns):
            table = self.columns[self._column_seek]
            self._column_seek += 1
            return table
        else:
            raise StopIteration

    def __contains__(self, column_name: str) -> bool:
        column = next(filter(
            lambda column: column.name == column_name, self.columns
        ), None)
        return column is not None

    def __getitem__(self, column_name: str) -> Column:
        column = next(filter(
            lambda column: column.name == column_name, self.columns
        ), None)
        if column is None:
            raise ColumnNotFoundException(column_name)
        else:
            return column


class Database:
    def __init__(
            self, connection: Connection, database: str,
            table: Optional[str] = None) -> None:
        self.name = database
        self.canonical_name = f'`{self.name}`'
        self.tables = []
        self._table_seek = 0
        self.connection = connection

        progress = Progress(
            TextColumn(
                '[bold blue]Analyze database {task.fields[database].name}',
                justify='right'),
            BarColumn(bar_width=None),
            '[progress.percentage]{task.percentage:>3.1f}%',
            '•',
            MofNCompleteColumn(),
            '•',
            TimeRemainingColumn(),
            '•',
            TimeElapsedColumn()
        )

        if table is None:
            tables = self.connection.execute(text(
                f"SHOW TABLES FROM `{database}`"
            ))
        else:
            tables = self.connection.execute(text(
                f"SHOW TABLES FROM `{database}` LIKE :table"
            ), table=table)

        task_id = progress.add_task(
            f'Analyze database {self.canonical_name}...', database=self)

        rows = tables.fetchall()
        progress.update(task_id, total=len(rows))
        progress.start()

        for row in rows:
            table = Table(self, row[0])
            self.tables.append(table)
            progress.update(task_id, advance=1)

        progress.stop()

    def query(self, sql: str, **kwargs) -> CursorResult:
        return self.connection.execute(text(sql), **kwargs)

    def describe(self) -> None:
        print(
                f"{self.name}\n" +
                ("=" * len(self.name)) + "\n" +
                "\n".join([str(table) for table in self])
        )

    def __str__(self):
        return f'`{self.name}`'

    def __iter__(self):
        self._table_seek = 0
        return self

    def __next__(self) -> Table:
        if self._table_seek < len(self.tables):
            table = self.tables[self._table_seek]
            self._table_seek += 1
            return table
        else:
            raise StopIteration

    def __getitem__(self, table_name: str) -> Table:
        table = next(filter(
            lambda table: table.name == table_name, self.tables), None)
        if table is None:
            raise ColumnNotFoundException(table_name)
        else:
            return table

    def __len__(self):
        return len(self.tables)


@dataclass
class TableConstraints:
    table: Table
    constraints: Dict[str, List[Column]] = field(default_factory=dict)

    def __post_init__(self):
        self.columns = []
        for columns in self.constraints.values():
            for column in columns:
                if column not in self.columns:
                    self.columns.append(column)

    def __setitem__(self, key, value):
        self.constraints[key] = value

    def __getitem__(self, item) -> List[Column]:
        return list(self.constraints.values())[item]

    def __contains__(self, item) -> bool:
        return item in self.constraints

    def describe(self):
        print(f'Describe constraints on {self.table.name}')
        for key, columns in self.constraints.items():
            columns_name = list(map(lambda column: column.name, columns))
            print(f' * {key} -> {columns_name}')


@dataclass
class TableUniqueConstraints(TableConstraints):
    def test(self, row: Dict[str, Any]) -> bool:
        for key, columns in self.constraints.items():
            conditions = []
            params = {}
            for column in columns:
                conditions.append(f"{column.canonical_name} = :{column.name}")
                params[column.name] = row[column.name]

            database = self.table.database
            count = database.query(f"""
                SELECT COUNT(*) FROM {self.table.canonical_name}
                WHERE {' AND '.join(conditions)}
            """, **params).fetchone()

            if count[0] > 0:
                return False

        return True

    """ 현재 테이블의 레코드들 중 전달된 컬럼들만 선택하여 중복이 제거된 레코드들을 환반
    """
    def fetch_unique_rows(
            self, columns: List[Union[str, Column]]) -> List[Row]:
        for index, column in enumerate(columns):
            if isinstance(column, str):
                columns[index] = self.table[column]

        columns_canonical_name = list(map(
            lambda column: column.canonical_name, columns))

        return self.table.database.query(f"""
            SELECT DISTINCT {', '.join(columns_canonical_name)}
            FROM {self.table.canonical_name}
        """).fetchall()

    """ 전달된 컬럼들을 참조하고 있는 필드들의 값 중에 현재 테이블에서 아직 사용하지 않은
        값들의 쌍을 조회하기 위해 사용되는 WHERE절 및 파라미터 반환
        (내부 로직에서만 사용되는 특수한 용도로 modifier는 protected)
    """
    def _where_foreign_values_not_in(
            self, columns: List[Column]) -> Optional[Tuple[str, dict]]:
        params = {}
        exists_value_sets = []
        for row in self.fetch_unique_rows(columns):
            expr_list = []
            for column in columns:
                if column.referenced_to is None:
                    continue
                ref_column = self.table.references_to([column])[0]
                param_name = f'param{len(params)}'
                expr_list.append(
                    f"{ref_column.canonical_name} <> :{param_name}")
                params[param_name] = row[column.name]
            exists_value_sets.append(f"({' AND '.join(expr_list)})")

        if len(exists_value_sets) > 0:
            return ' AND '.join(exists_value_sets), params
        else:
            return None

    """ 전달된 컬럼들을 참조하고 있는 필드들의 값 중에 현재 테이블에서 아직 사용하지 않은
        (현재 테이블 유니크 인덱스에 포함되지 않는)값들의 쌍을 반환
        여기서 dict의 키는 참조하는 테이블이 아닌 현재 테이블의 필드명으로 전달됨
        ```
        [
            {'field1': 1, 'field2': 2},
            {'field1': 2, 'field2': 2},
            {'field4': 2, 'field5': 2, 'field6': 2},
            {'field4': 2, 'field5': 2, 'field6': 3},
            {'field1': 2, 'field2': 3},
        ]
        ```
    """
    def fetch_available_foreign_values(
            self, columns: List[Column]) -> List[dict]:
        ref_columns = self.table.references_to(columns)

        if len(ref_columns) < 1:
            return []

        available_rows = []
        ref_tables = list(map(lambda column: column.table, ref_columns))
        ref_columns_canonical_name = list(map(
            lambda column: column.canonical_name, ref_columns))
        ref_tables_canonical_name = []
        for table in ref_tables:
            if table.canonical_name not in ref_tables_canonical_name:
                ref_tables_canonical_name.append(table.canonical_name)

        sql = f"""
            SELECT DISTINCT {', '.join(ref_columns_canonical_name)}
            FROM {', '.join(ref_tables_canonical_name)}
        """

        query = self._where_foreign_values_not_in(columns)
        params = {}
        if query is not None:
            where, params = query
            sql += f" WHERE {where}"

        sql += f" GROUP BY {', '.join(ref_columns_canonical_name)}"

        for row in self.table.database.query(sql, **params):
            available_row = {}
            for index, ref_column in enumerate(ref_columns):
                for column in columns:
                    if column.is_referenced_to(ref_column):
                        available_row[column.name] = row[index]
            available_rows.append(available_row)

        return available_rows

    def fetch_available_foreign_rows_set(self) -> Dict[str, List[dict]]:
        available_foreign_rows_set = {}
        for index, columns in self.constraints.items():
            # 유니크인덱스 중 외래키인 필드 처리
            available_foreign_rows_set[index] = \
                self.fetch_available_foreign_values(columns)
        return available_foreign_rows_set

    def fetch_available_foreign_rows(self) -> List[dict]:
        available_rows = []
        for index, columns in self.constraints.items():
            # 유니크인덱스 중 외래키인 필드 처리
            available_rows += self.fetch_available_foreign_values(columns)
        return available_rows

    def __len__(self):
        return len(self.constraints)


class UniqueForeignScope:
    def __init__(self, foreign_rows_set: Dict[str, List[dict]]):
        self.rows_set = foreign_rows_set

    def get_column_values(self, column: Column) -> List[Any]:
        values = []
        for key, rows in self.rows_set.items():
            for row in rows:
                if column.name in row and row[column.name] not in values:
                    values.append(row[column.name])

        return values

    def get_random_value_on_column(self, column: Column) -> Optional[Any]:
        values = self.get_column_values(column)
        length = len(values)
        if length > 0:
            index = random.randint(0, length - 1)
            return values[index]
        else:
            return None

    def scope_column_value(self, column: Column, value: Any) -> None:
        scoped_rows_set = {}
        for key, rows in self.rows_set.items():
            scoped_rows = []
            for row in rows:
                if column.name in row and row[column.name] == value:
                    scoped_rows.append(row)
            scoped_rows_set[key] = scoped_rows
        self.rows_set = scoped_rows_set

    def random_scope_column(self, column: Column) -> Optional[Any]:
        value = self.get_random_value_on_column(column)
        if value is not None:
            self.scope_column_value(column, value)
        return value
