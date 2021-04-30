from sqllineage.runner import LineageRunner
from .helpers import helper


def test_use():
    helper("USE db1")


def test_another_three_tiered_example():
    sql = """SELECT 1 as output_column
        FROM FOO.GITHUB.LABEL b
        LEFT JOIN FOO.GITHUB.PULL_REQUEST c ON c.ID = b.ID
        LEFT JOIN ( SELECT com.REPOSITORY_ID as ID FROM FOO.GITHUB.COMMIT com )
        as mp ON mp.ID = b.ID"""
    helper(sql, {"foo.github.label", "foo.github.pull_request", "foo.github.commit"})

def test_date_table():
    helper("""
        WITH spine as (select 1 as the_date from table(generator(rowcount => 10000)) where the_date < 2)
        select the_date from spine join mozart.foo f on spine.the_date = f.the_date
    """, {"mozart.foo"})


def test_table_generator_no_space_before_parenthesis():
    sql = """select 1 from table(generator(rowcount => 10000)) where 1 <= 2"""
    helper(sql, {})


def test_table_generator_with_space_before_parenthesis():
    sql = """select 1 from table (generator(rowcount => 10000)) where 1 <= 2"""
    helper(sql, {})


def test_table_generator_new_regression():
    helper("""WITH date_series as (
         select DATEADD(DAY, SEQ4(), '1970-01-01') AS date_value
           FROM TABLE(GENERATOR(rowcount => 10000)) -- Comment breaking parser
       )
       select date_value as date from date_series""", {})


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
