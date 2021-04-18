from sqllineage.runner import LineageRunner

from .helpers import assert_table_lineage_equal


def test_use():
    assert_table_lineage_equal("USE db1")


def test_table_name_case():
    assert_table_lineage_equal(
        """insert overwrite table tab_a
select * from tab_b
union all
select * from TAB_B""",
        {"tab_b"},
        {"tab_a"},
    )


def test_create():
    assert_table_lineage_equal("CREATE TABLE tab1 (col1 STRING)", None, {"tab1"})


def test_create_if_not_exist():
    assert_table_lineage_equal(
        "CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)", None, {"tab1"}
    )


def test_create_bucket_table():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 USING parquet CLUSTERED BY (col1) INTO 500 BUCKETS",
        None,
        {"tab1"},
    )


def test_create_as():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 AS SELECT * FROM tab2", {"tab2"}, {"tab1"}
    )


def test_table_generator_new_regression():
    assert_table_lineage_equal(
        """WITH date_series as (
         select DATEADD(DAY, SEQ4(), '1970-01-01') AS date_value
           FROM TABLE(GENERATOR(rowcount => 10000)) -- Comment breaking parser
       )
       select date_value as date from date_series""",
        {},
        {},
    )


def test_create_like():
    assert_table_lineage_equal("CREATE TABLE tab1 LIKE tab2", {"tab2"}, {"tab1"})


def test_create_select():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 SELECT * FROM tab2", {"tab2"}, {"tab1"}
    )


def test_create_after_drop():
    assert_table_lineage_equal(
        "DROP TABLE IF EXISTS tab1; CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)",
        None,
        {"tab1"},
    )


def test_create_using_serde():
    # Check https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RowFormats&SerDe
    # here with is not an indicator for CTE
    assert_table_lineage_equal(
        """CREATE TABLE apachelog (
  host STRING,
  identity STRING,
  user STRING,
  time STRING,
  request STRING,
  status STRING,
  size STRING,
  referer STRING,
  agent STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^]*) ([^]*) ([^]*) (-|\\[^\\]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\".*\") ([^ \"]*|\".*\"))?"
)
STORED AS TEXTFILE""",  # noqa
        None,
        {"apachelog"},
    )


def test_update():
    assert_table_lineage_equal(
        "UPDATE tab1 SET col1='val1' WHERE col2='val2'", None, {"tab1"}
    )


def test_update_with_join():
    assert_table_lineage_equal(
        "UPDATE tab1 a INNER JOIN tab2 b ON a.col1=b.col1 SET a.col2=b.col2",
        {"tab2"},
        {"tab1"},
    )


def test_copy_from_table():
    assert_table_lineage_equal(
        "COPY tab1 FROM tab2",
        {"tab2"},
        {"tab1"},
    )


def test_drop():
    assert_table_lineage_equal("DROP TABLE IF EXISTS tab1", None, None)


def test_drop_with_comment():
    assert_table_lineage_equal(
        """--comment
DROP TABLE IF EXISTS tab1""",
        None,
        None,
    )


def test_drop_after_create():
    assert_table_lineage_equal(
        "CREATE TABLE IF NOT EXISTS tab1 (col1 STRING);DROP TABLE IF EXISTS tab1",
        None,
        None,
    )


def test_drop_tmp_tab_after_create():
    sql = """create table tab_a as select * from tab_b;
insert overwrite table tab_c select * from tab_a;
drop table tab_a;"""
    assert_table_lineage_equal(sql, {"tab_b"}, {"tab_c"})


def test_new_create_tab_as_tmp_table():
    sql = """create table tab_a as select * from tab_b;
create table tab_c as select * from tab_a;"""
    assert_table_lineage_equal(sql, {"tab_b"}, {"tab_c"})


def test_alter_table_rename():
    assert_table_lineage_equal("alter table tab1 rename to tab2;", None, None)


def test_alter_table_exchange_partition():
    """
    See https://cwiki.apache.org/confluence/display/Hive/Exchange+Partition for language manual
    """
    assert_table_lineage_equal(
        "alter table tab1 exchange partition(pt='part1') with table tab2",
        {"tab2"},
        {"tab1"},
    )


def test_swapping_partitions():
    """
    See https://www.vertica.com/docs/10.0.x/HTML/Content/Authoring/AdministratorsGuide/Partitions/SwappingPartitions.htm
    for language specification
    """
    assert_table_lineage_equal(
        "select swap_partitions_between_tables('staging', 'min-range-value', 'max-range-value', 'target')",
        {"staging"},
        {"target"},
    )


def test_alter_target_table_name():
    assert_table_lineage_equal(
        "insert overwrite tab1 select * from tab2; alter table tab1 rename to tab3;",
        {"tab2"},
        {"tab3"},
    )


def test_refresh_table():
    assert_table_lineage_equal("refresh table tab1", None, None)


def test_cache_table():
    assert_table_lineage_equal("cache table tab1", None, None)


def test_uncache_table():
    assert_table_lineage_equal("uncache table tab1", None, None)


def test_uncache_table_if_exists():
    assert_table_lineage_equal("uncache table if exists tab1", None, None)


def test_truncate_table():
    assert_table_lineage_equal("truncate table tab1", None, None)


def test_delete_from_table():
    assert_table_lineage_equal("delete from table tab1", None, None)


def test_show_create_table():
    assert_table_lineage_equal("show create table tab1", None, None)


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


def test_another_three_tiered_example():
    sql = """SELECT 1 as output_column
        FROM FOO.GITHUB.LABEL b
        LEFT JOIN FOO.GITHUB.PULL_REQUEST c ON c.ID = b.ID
        LEFT JOIN ( SELECT com.REPOSITORY_ID as ID FROM FOO.GITHUB.COMMIT com )
        as mp ON mp.ID = b.ID"""
    assert_table_lineage_equal(
        sql, {"foo.github.label", "foo.github.pull_request", "foo.github.commit"}, {}
    )


def test_date_table():
    assert_table_lineage_equal(
        """
        WITH spine as (select 1 as the_date from table(generator(rowcount => 10000)) where the_date < 2)
        select the_date from spine join mozart.foo f on spine.the_date = f.the_date
    """,
        {"mozart.foo"},
        {},
    )


def test_another_date_table():
    sql = """
        -- Pick a reasonable starting date for the given business
        -- Ends one year from today
        with
        date_spine as (
          select
            dateadd('day', seq4(), '2020-01-01'::date)::date as the_date
          from
            table
              (generator(rowcount => 10000))
            where the_date <= dateadd('year', 1, current_date())
        )
        select
          -- dates
          the_date AS "date", -- for backwards compatibility
          the_date,
          dateadd('day', -1, the_date) as lag_date,
          dateadd('week', -1, the_date) as lag_week,
          dateadd('month', -1, the_date) as lag_month,
          dateadd('year', -1, the_date) as lag_year,
          last_day(the_date, 'week') AS eow,
          last_day(the_date, 'month') AS eom,
          last_day(the_date, 'quarter') AS eoq,
          last_day(the_date, 'year')  AS eoy,
          -- numbers
          date_part('year', the_date) as the_year,
          date_part('month', the_date) as the_month,
          date_part('day', the_date) as dom,
          date_part('doy', the_date) as doy,
          date_part('dow_iso', the_date) as dow,
          date_part('week', the_date) as the_week,
          date_part('quarter', the_date) as the_quarter,
          -- bools
          case when the_date = current_date() then true else false end as is_latest_date,
          case when the_date > current_date() then true else false end as is_future_date,
          case when date_part('dow_iso', the_date) < 6 then true else false end as is_weekday,
          case when the_date = last_day(the_date, 'week') then true else false end as is_eow,
          case when the_date = last_day(the_date, 'month') then true else false end as is_eom,
          case when the_date = last_day(the_date, 'quarter') then true else false end as is_eoq,
          case when the_date = last_day(the_date, 'year') then true else false end as is_eoy,
          case when date_part('year', the_date) % 4 = 0 then true else false end as is_leap_year,
          null as is_holiday,
          null as is_mozart_holiday,
          -- names
          dayname(the_date) as day_name,
          null as holiday_name
        from date_spine"""
    assert_table_lineage_equal(sql, {}, {})


def test_table_generator_no_space_before_parenthesis():
    sql = """select 1 from table(generator(rowcount => 10000)) where 1 <= 2"""
    assert_table_lineage_equal(sql, {}, {})


def test_table_generator_with_space_before_parenthesis():
    sql = """select 1 from table (generator(rowcount => 10000)) where 1 <= 2"""
    assert_table_lineage_equal(sql, {}, {})


def test_sql_with_no_table():
    sql = """select 1 as test_table"""
    assert_table_lineage_equal(sql, {}, {})
