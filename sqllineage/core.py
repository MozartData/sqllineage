import re
from typing import Set, TYPE_CHECKING, Tuple

from sqlparse.sql import (
    Comment,
    Comparison,
    Function,
    Identifier,
    IdentifierList,
    Parenthesis,
    Statement,
    TokenList,
)
from sqlparse.tokens import Number

from sqllineage.exceptions import SQLLineageException
from sqllineage.models import Table

SOURCE_TABLE_TOKENS = (
    r"FROM",
    # inspired by https://github.com/andialbrecht/sqlparse/blob/master/sqlparse/keywords.py
    r"((LEFT\s+|RIGHT\s+|FULL\s+)?(INNER\s+|OUTER\s+|STRAIGHT\s+)?|(CROSS\s+|NATURAL\s+)?)?JOIN",
)
TEMP_TABLE_TOKENS = ("WITH",)


class LineageResult:
    """
    Statement Level Lineage Result.

    LineageResult will hold attributes like read, write, rename, drop, intermediate.

    Each of them is a Set[:class:`sqllineage.models.Table`] except for rename.

    For rename, it a Set[Tuple[:class:`sqllineage.models.Table`, :class:`sqllineage.models.Table`]], with the first
    table being original table before renaming and the latter after renaming.

    This is the most atomic representation of lineage result.
    """

    __slots__ = ["read", "write", "rename", "drop", "intermediate"]
    if TYPE_CHECKING:
        read = write = drop = intermediate = set()  # type: Set[Table]
        rename = set()  # type: Set[Tuple[Table, Table]]

    def __init__(self) -> None:
        for attr in self.__slots__:
            setattr(self, attr, set())

    def __add__(self, other):
        lineage_result = LineageResult()
        for attr in self.__slots__:
            setattr(
                lineage_result, attr, getattr(self, attr).union(getattr(other, attr))
            )
        return lineage_result

    def __str__(self):
        return "\n".join(
            f"table {attr}: {sorted(getattr(self, attr), key=lambda x: str(x)) if getattr(self, attr) else '[]'}"
            for attr in self.__slots__
        )

    def __repr__(self):
        return str(self)


class LineageAnalyzer:
    """SQL Statement Level Lineage Analyzer."""

    def __init__(self) -> None:
        self._lineage_result = LineageResult()

    def analyze(self, stmt: Statement) -> LineageResult:
        """
        to analyze the Statement and store the result into :class:`LineageResult`.

        :param stmt: a SQL statement parsed by `sqlparse`
        """
        if stmt.get_type() == "DROP":
            self._extract_from_ddl_drop(stmt)
        elif stmt.get_type() == "ALTER":
            self._extract_from_ddl_alter(stmt)
        elif (
            stmt.get_type() == "DELETE"
            or stmt.token_first(skip_cm=True).normalized == "TRUNCATE"
            or stmt.token_first(skip_cm=True).normalized.upper() == "REFRESH"
            or stmt.token_first(skip_cm=True).normalized == "CACHE"
            or stmt.token_first(skip_cm=True).normalized.upper() == "UNCACHE"
        ):
            pass
        else:
            # DML parsing logic also applies to CREATE DDL
            self._extract_from_dml(stmt)
        return self._lineage_result

    def _extract_from_ddl_drop(self, stmt: Statement) -> None:
        for table in {
            Table.create(t) for t in stmt.tokens if isinstance(t, Identifier)
        }:
            self._lineage_result.drop.add(table)

    def _extract_from_ddl_alter(self, stmt: Statement) -> None:
        tables = [Table.create(t) for t in stmt.tokens if isinstance(t, Identifier)]
        keywords = [t for t in stmt.tokens if t.is_keyword]
        if any(k.normalized == "RENAME" for k in keywords) and len(tables) == 2:
            self._lineage_result.rename.add((tables[0], tables[1]))

    def _extract_from_dml(self, token: TokenList) -> None:
        source_table_token_flag = False
        temp_table_token_flag = False
        prev_token_is_table = False
        for sub_token in token.tokens:
            if self.__token_negligible_before_tablename(sub_token):
                continue

            if isinstance(sub_token, TokenList):
                self._extract_from_dml(sub_token)

            if sub_token.normalized == 'TABLE':
                prev_token_is_table = True
            if sub_token.is_keyword:
                if any(
                    re.match(regex, sub_token.normalized)
                    for regex in SOURCE_TABLE_TOKENS
                ) and not isinstance(sub_token.parent.parent, Function):
                    # SELECT trim(BOTH '  ' FROM '  abc  '); Here FROM is not a source table flag
                    source_table_token_flag = True
                elif sub_token.normalized in TEMP_TABLE_TOKENS:
                    temp_table_token_flag = True
                continue

            if prev_token_is_table:
                if isinstance(sub_token, Parenthesis):
                    prev_token_is_table = False

            if source_table_token_flag:
                self._handle_source_table_token(sub_token)
                source_table_token_flag = False
            elif temp_table_token_flag:
                self._handle_temp_table_token(sub_token)
                temp_table_token_flag = False

    def _handle_source_table_token(self, sub_token: TokenList) -> None:
        if isinstance(sub_token, Identifier):
            if isinstance(sub_token.token_first(skip_cm=True), Parenthesis):
                # SELECT col1 FROM (SELECT col2 FROM tab1) dt, the subquery will be parsed as Identifier
                # and this Identifier's get_real_name method would return alias name dt
                # referring https://github.com/andialbrecht/sqlparse/issues/218 for further information
                pass
            else:
                self._lineage_result.read.add(Table.create(sub_token))
            return

        if isinstance(sub_token, IdentifierList):
            # This is to support join in ANSI-89 syntax
            for token in sub_token.tokens:
                # when real name and alias name are the same, it means subquery here
                if (
                    isinstance(token, Identifier)
                    and token.get_real_name() != token.get_alias()
                ):
                    self._lineage_result.read.add(Table.create(token))
            return

        if isinstance(sub_token, Parenthesis):
            # SELECT col1 FROM (SELECT col2 FROM tab1), the subquery will be parsed as Parenthesis
            # This syntax without alias for subquery is invalid in MySQL, while valid for SparkSQL
            return

        # If there is a newline immediately preceded by comments and whitespace, trim all the comments and whitespace
        # or else we might throw an unnecessary exception.
        tokens_to_trim = sub_token.tokens
        while len(tokens_to_trim) > 0 and self.__token_negligible_before_tablename(tokens_to_trim[-1]):
            tokens_to_trim = tokens_to_trim[:-1]

        if isinstance(sub_token, Function) and len(tokens_to_trim) == 2:
            if isinstance(tokens_to_trim[1], Parenthesis) and tokens_to_trim[0].normalized.upper() == "TABLE":
                # Punt on finding stuff inside this wrapper.
                return

        raise SQLLineageException(
            "An Identifier is expected, got %s[value: %s] instead"
            % (type(sub_token).__name__, sub_token)
        )

    def _handle_temp_table_token(self, sub_token: TokenList) -> None:
        if isinstance(sub_token, Identifier):
            self._lineage_result.intermediate.add(Table.create(sub_token))
            self._extract_from_dml(sub_token)
        elif isinstance(sub_token, IdentifierList):
            for temp_tab_token in sub_token:
                if isinstance(temp_tab_token, Identifier):
                    self._lineage_result.intermediate.add(Table.create(temp_tab_token))
                    self._extract_from_dml(temp_tab_token)
        else:
            raise SQLLineageException(
                "An Identifier or IdentifierList is expected, got %s[value: %s] instead"
                % (type(sub_token).__name__, sub_token)
            )

    @classmethod
    def __token_negligible_before_tablename(cls, token: TokenList) -> bool:
        return token.is_whitespace or isinstance(token, Comment)
