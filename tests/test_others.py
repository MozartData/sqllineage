from sqllineage.runner import LineageRunner
from .helpers import helper


def test_use():
    helper("USE db1")


def test_table_name_case():
    helper(
        """insert overwrite table tab_a
select * from tab_b
union all
select * from TAB_B""",
        {"tab_b"},
        {"tab_a"},
    )


def test_create():
    helper("CREATE TABLE tab1 (col1 STRING)", None, {"tab1"})


def test_create_if_not_exist():
    helper("CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)", None, {"tab1"})


def test_create_bucket_table():
    helper(
        "CREATE TABLE tab1 USING parquet CLUSTERED BY (col1) INTO 500 BUCKETS",
        None,
        {"tab1"},
    )


def test_create_as():
    helper("CREATE TABLE tab1 AS SELECT * FROM tab2", {"tab2"}, {"tab1"})


def test_create_as_dwh():
    helper("CREATE TABLE tab1 AS SELECT * FROM foo.bar.tab2", {"foo.bar.tab2"}, {"tab1"})


def test_another_three_tiered_example():
    sql = """SELECT 1 as output_column
        FROM FOO.GITHUB.LABEL b
        LEFT JOIN FOO.GITHUB.PULL_REQUEST c ON c.ID = b.ID
        LEFT JOIN ( SELECT com.REPOSITORY_ID as ID FROM FOO.GITHUB.COMMIT com )
        as mp ON mp.ID = b.ID"""
    helper(sql, {"foo.github.label", "foo.github.pull_request", "foo.github.commit"}, {})

def test_date_table():
    helper("""
        WITH spine as (select 1 as the_date from table(generator(rowcount => 10000)) where the_date < 2)
        select the_date from spine join mozart.foo f on spine.the_date = f.the_date
    """, {"mozart.foo"}, {})


def test_table_generator_no_space_before_parenthesis():
    sql = """select 1 from table(generator(rowcount => 10000)) where 1 <= 2"""
    helper(sql, {}, {})


def test_table_generator_with_space_before_parenthesis():
    sql = """select 1 from table (generator(rowcount => 10000)) where 1 <= 2"""
    helper(sql, {}, {})


def test_table_generator_new_regression():
    helper("""WITH date_series as (
         select DATEADD(DAY, SEQ4(), '1970-01-01') AS date_value
           FROM TABLE(GENERATOR(rowcount => 10000)) -- Comment breaking parser
       )
       select date_value as date from date_series""", {}, {})


def test_create_like():
    helper("CREATE TABLE tab1 LIKE tab2", {"tab2"}, {"tab1"})


def test_create_select():
    helper("CREATE TABLE tab1 SELECT * FROM tab2", {"tab2"}, {"tab1"})


def test_create_after_drop():
    helper(
        "DROP TABLE IF EXISTS tab1; CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)",
        None,
        {"tab1"},
    )


def test_update():
    helper("UPDATE tab1 SET col1='val1' WHERE col2='val2'", None, {"tab1"})


def test_update_with_join():
    helper(
        "UPDATE tab1 a INNER JOIN tab2 b ON a.col1=b.col1 SET a.col2=b.col2",
        {"tab2"},
        {"tab1"},
    )


def test_drop():
    helper("DROP TABLE IF EXISTS tab1", None, None)


def test_drop_with_comment():
    helper(
        """--comment
DROP TABLE IF EXISTS tab1""",
        None,
        None,
    )


def test_drop_after_create():
    helper(
        "CREATE TABLE IF NOT EXISTS tab1 (col1 STRING);DROP TABLE IF EXISTS tab1",
        None,
        None,
    )


def test_drop_tmp_tab_after_create():
    sql = """create table tab_a as select * from tab_b;
insert overwrite table tab_c select * from tab_a;
drop table tab_a;"""
    helper(sql, {"tab_b"}, {"tab_c"})


def test_new_create_tab_as_tmp_table():
    sql = """create table tab_a as select * from tab_b;
create table tab_c as select * from tab_a;"""
    helper(sql, {"tab_b"}, {"tab_c"})


def test_alter_table_rename():
    helper("alter table tab1 rename to tab2;", None, None)


def test_alter_target_table_name():
    helper(
        "insert overwrite tab1 select * from tab2; alter table tab1 rename to tab3;",
        {"tab2"},
        {"tab3"},
    )


def test_refresh_table():
    helper("refresh table tab1", None, None)


def test_cache_table():
    helper("cache table tab1", None, None)


def test_uncache_table():
    helper("uncache table tab1", None, None)


def test_uncache_table_if_exists():
    helper("uncache table if exists tab1", None, None)


def test_truncate_table():
    helper("truncate table tab1", None, None)


def test_delete_from_table():
    helper("delete from table tab1", None, None)


def test_split_statements():
    sql = "SELECT * FROM tab1; SELECT * FROM tab2;"
    assert len(LineageRunner(sql).statements()) == 2


def test_split_statements_with_heading_and_ending_new_line():
    sql = "\nSELECT * FROM tab1;\nSELECT * FROM tab2;\n"
    assert len(LineageRunner(sql).statements()) == 2


def test_split_statements_with_comment():
    sql = """SELECT 1;

-- SELECT 2;"""
    assert len(LineageRunner(sql).statements()) == 1


def test_statements_trim_comment():
    comment = "------------------\n"
    sql = "select * from dual;"
    assert LineageRunner(comment + sql).statements(strip_comments=True)[0] == sql


def test_split_statements_with_show_create_table():
    sql = """SELECT 1;

SHOW CREATE TABLE tab1;"""
    assert len(LineageRunner(sql).statements()) == 2


def test_split_statements_with_desc():
    sql = """SELECT 1;

DESC tab1;"""
    assert len(LineageRunner(sql).statements()) == 2
