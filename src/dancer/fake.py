from __future__ import annotations

from typing import Optional, Callable, Any, Union, Dict, List
from dataclasses import dataclass, field
import random
from faker import Faker
from rich.console import Console
from rich.progress import (
    Progress, TextColumn, BarColumn, MofNCompleteColumn, TimeRemainingColumn,
    TimeElapsedColumn
)
from .mysql import Column, Table, Database, UniqueForeignScope


"""
:empty_ratio: 빈 값이 발생할 확률

:nullable_ratio: null값이 발생할 확률

:prevent_negative: 음수값 방지
"""


@dataclass
class FakePolicy:
    fake: Faker = field(default_factory=Faker)
    empty_ratio: float = field(default=0.01)
    nullable_ratio: float = field(default=0.01)
    prevent_negative: Optional[bool] = field(default=None)

    def is_null(self):
        return random.random() < self.nullable_ratio

    def is_empty(self):
        return random.random() < self.empty_ratio

    def kwargs(self):
        return {
            'fake': self.fake,
            'empty_ratio': self.empty_ratio,
            'nullable_ratio': self.nullable_ratio,
            'prevent_negative': self.prevent_negative
        }


@dataclass
class FakeColumn(FakePolicy):
    column: Column = None
    func: Optional[Callable[[FakeColumn], Any]] = field(default=None)
    args: Union[list, tuple, dict] = field(default_factory=list)

    def __post_init__(self):
        kwargs = self.__dict__.copy()
        del kwargs['column'], kwargs['func'], kwargs['args']
        super(FakeColumn, self).__init__(**kwargs)

    def __call__(self, *args, **kwargs):
        if self.func is None:
            return getattr(self, self.column.type)()
        else:
            fake_func = self.func(self)
            if isinstance(fake_func, str):
                return lambda: fake_func
            elif self.args is None or len(self.args) < 1:
                return fake_func()
            elif isinstance(self.args, dict):
                return fake_func(**self.args)
            elif isinstance(self.args, list) or isinstance(self.args, tuple):
                return fake_func(*self.args)
            else:
                return fake_func(self.args)

    def tinyint(self) -> int:
        if self.column.unsigned:
            return random.randint(0, 255)
        elif self.prevent_negative:
            return random.randint(0, 127)
        else:
            return random.randint(-128, 127)

    def smallint(self) -> int:
        if self.column.unsigned:
            return random.randint(0, 65535)
        elif self.prevent_negative:
            return random.randint(0, 32767)
        else:
            return random.randint(-32768, 32767)

    def mediumint(self) -> int:
        if self.column.unsigned:
            return random.randint(0, 16777215)
        elif self.prevent_negative:
            return random.randint(0, 8388607)
        else:
            return random.randint(-8388608, 8388607)

    def int(self) -> int:
        if self.column.unsigned:
            return random.randint(0, 4294967295)
        elif self.prevent_negative:
            return random.randint(0, 2147483647)
        else:
            return random.randint(-2147483648, 2147483647)

    def bigint(self) -> int:
        if self.column.unsigned:
            return random.randint(0, 18446744073709551615)
        elif self.prevent_negative:
            return random.randint(0, 9223372036854775807)
        else:
            return random.randint(-9223372036854775808, 9223372036854775807)

    def char(self) -> str:
        return ''.join(self.fake.random_letters(length=self.column.size))

    def varchar(self) -> str:
        if self.is_empty():
            return ''

        if self.column.size < 5:
            return ''.join(self.fake.random_letters(
                length=self.fake.random_int(min=1, max=self.column.size)))
        else:
            return self.fake.text(max_nb_chars=self.column.size)

    def text(self) -> str:
        if self.is_empty():
            return ''

        return self.fake.sentence()

    def longtext(self) -> str:
        if self.is_empty():
            return ''

        return self.fake.sentence()

    def float(self) -> float:
        if self.column.size is None:
            return random.random() * self.fake.random_int(min=1, max=12)
        else:
            precision, scale = self.column.size
            if scale > 0:
                format = ('#' * (precision - scale)) + '.' + ('#' * scale)
            else:
                format = '#' * precision
            return float(self.fake.bothify(format))

    def decimal(self) -> float:
        precision, scale = self.column.size
        if scale > 0:
            format = ('#' * (precision - scale)) + '.' + ('#' * scale)
        else:
            format = '#' * precision
        return float(self.fake.bothify(format))

    def enum(self) -> str:
        return self.fake.random_element(elements=self.column.size)

    def set(self) -> str:
        return self.fake.random_elements(elements=self.column.size)

    def date(self) -> datetime:
        return self.fake.date()

    def datetime(self) -> datetime:
        return self.fake.date_time()

    def timestamp(self) -> datetime:
        return self.fake.date_time()


@dataclass
class FakeRow:
    fake_table: FakeTable
    fields: Dict[str, Any] = field(default_factory=dict)

    def insert_query(self):
        table = self.fake_table.table
        database = table.database

        values_set = '(' + (', '.join(list(map(
            lambda name: ':' + name, self.fields.keys()
        )))) + ')'

        return f"""
            INSERT INTO `{database.name}`.`{table.name}`
                (`{"`, `".join(table.fillable_fields())}`)
            VALUES {values_set}
        """

    def insert(self):
        table = self.fake_table.table
        table.database.query(self.insert_query(), **self.fields)

    def __contains__(self, field_name: str) -> bool:
        return field_name in self.fields

    def __getitem__(self, field_name: str) -> Any:
        return self.fields[field_name]

    def __setitem__(self, key, value):
        self.fields[key] = value

    def __str__(self):
        return str(self.fields)


@dataclass
class FakeTable(FakePolicy):
    table: Table = None
    num_of_rows: int = field(default=1)
    columns: Dict[str, FakeColumn] = field(default_factory=dict)

    """ FakeRow 생성
    :random_seed: 외래키 제약이 걸려있는 필드의 데이터를 연관 테이블에서 가져올 때 사용
    """
    def new_fake_row(
            self, retries: int = 1,
            random_seed: Optional[int] = None) -> Optional[FakeRow]:
        fake_row = FakeRow(fake_table=self)

        unique_constraints = self.table.get_unique_constraints()
        available_foreign_rows_set = \
            unique_constraints.fetch_available_foreign_rows_set()
        foreign_scopes = UniqueForeignScope(available_foreign_rows_set)

        for _ in range(retries):
            for column in self.table.fillable_columns():
                if column.name in self.columns:
                    fake_column = self.columns[column.name]
                else:
                    fake_column = FakeColumn(**self.kwargs(), column=column)

                if column.nullable and fake_column.is_null():
                    fake_value = None
                elif column.referenced_to is not None:
                    if column not in unique_constraints.columns:
                        ref_table_name, ref_column_name = column.referenced_to
                        ref_fake_row = self.table.fetch_random_row(
                            ref_table_name, seed=random_seed)
                        if ref_fake_row is None:
                            fake_value = None
                        else:
                            fake_value = getattr(ref_fake_row, ref_column_name)
                    else:
                        fake_value = foreign_scopes.random_scope_column(column)
                elif hasattr(self, column.type):
                    factory_function = getattr(self, column.type)
                    fake_value = factory_function(column, self)
                else:
                    fake_value = fake_column()

                fake_row[column.name] = fake_value

            creatable = unique_constraints.test(fake_row.fields)
            if creatable:
                fake_row.insert()
                return fake_row
            else:
                print(f'{self.table.name} failed: {fake_row.fields}')

            if _ + 1 == retries:
                print(f'{self.table.name} failed: exhausted retry limit')

    def __contains__(self, column_name: str) -> bool:
        return column_name in self.columns

    def __getitem__(self, column_name: str) -> FakeColumn:
        return self.columns[column_name]


FakeColumnDefinitionType = Union[
    Callable[[Faker, Column], Any],
    Dict[str, Union[list, tuple, Optional[Callable[[Faker, Column], Any]]]]
]

FakeTableDefinitionType = Dict[str, Dict[str, Union[Any, FakeColumnDefinitionType]]]


@dataclass
class FakeFactory(FakePolicy):
    """
    :locale: This value will be passed to Faker's __init__
    """
    def __init__(
            self, locale: str, database: Database,
            table_definitions: Dict[str, FakeTableDefinitionType] = None,
            console: Optional[Console] = None,
            **kwargs) -> None:
        super(FakeFactory, self).__init__(**kwargs)
        self.fake = Faker(locale)
        self.database = database
        self.pre_produces_ref_tables = {}
        self.table_definitions = {}

        if console is None:
            console = Console()

        status = console.status('Preparing Fake Factory')
        status.start()

        if table_definitions is not None:
            for table_name, table_definition in table_definitions.items():
                table = self.database[table_name]
                column_definitions = {}
                if isinstance(table_definition, dict):
                    if isinstance(table_definition['columns'], dict):
                        for column_name, column_definition in table_definition[
                                'columns'].items():
                            kwargs = {}
                            if column_definition is None:
                                pass
                            elif isinstance(column_definition, dict):
                                kwargs = column_definition
                            elif callable(column_definition):
                                kwargs['func'] = column_definition
                            else:
                                kwargs['func'] = lambda fake_column:\
                                    (lambda: column_definition)
                            column_definitions[column_name] = FakeColumn(
                                **self.kwargs(),
                                column=table[column_name],
                                **kwargs
                            )

                kwargs = {}
                for key, value in table_definition.items():
                    if key != 'columns':
                        kwargs[key] = value

                self.table_definitions[table_name] = FakeTable(
                    table=table,
                    **self.kwargs(),
                    **kwargs,
                    columns=column_definitions
                )

        status.stop()

    def generate_fake_rows(
            self, table: Table) -> List[FakeRow]:
        if table in self.pre_produces_ref_tables:
            return self.pre_produces_ref_tables[table]

        for table_name in table.references_group_by_table().keys():
            _table = self.database[table_name]
            if table.name != _table.name:
                _rows = self.generate_fake_rows(_table)
                self.pre_produces_ref_tables[_table] = _rows

        if table.name in self.table_definitions:
            fake_table = self.table_definitions[table.name]
        else:
            fake_table = FakeTable(table=table, **self.kwargs())

        if fake_table.prevent_negative is None:
            fake_table.prevent_negative = self.prevent_negative

        fake_rows = []

        for _ in range(fake_table.num_of_rows):
            random_row_seed = random.randint(0, 2147483647)  # Keep same ref record for columns on the same records
            fake_row = fake_table.new_fake_row(100, random_row_seed)
            fake_rows.append(fake_row)

        return fake_rows

    def generate(self):
        progress = Progress(
            TextColumn(
                '[bold blue]Generate fake data {task.fields[factory].database.name}',
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
        task_id = progress.add_task(
            f'Generate fake data {self.database.name}...', factory=self)
        progress.update(task_id, total=len(self.database))
        progress.start()

        for table in self.database:
            self.generate_fake_rows(table)
            progress.update(task_id, advance=1)

        progress.stop()
