from sqllineage.models import Table
from sqllineage.runner import LineageRunner


def helper(sql, source_tables=None):
    lp = LineageRunner(sql)
    assert set(lp.source_tables) == (
        set() if source_tables is None else {Table(t) for t in source_tables}
    )
