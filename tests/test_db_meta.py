from pandas.testing import assert_frame_equal
from dwopt import Pg, Lt, Oc, make_eng
from dwopt._qry import _Qry
from dwopt.testing import _TEST_PG_URL, _TEST_LT_URL, _TEST_OC_URL


def test_db_meta_init(test_tbl):
    db, _ = test_tbl
    if isinstance(db, Pg):
        Pg(_TEST_PG_URL).list_tables()
        Pg(make_eng(_TEST_PG_URL)).list_tables()
    elif isinstance(db, Lt):
        Lt(_TEST_LT_URL).list_tables()
        Lt(make_eng(_TEST_LT_URL)).list_tables()
    elif isinstance(db, Oc):
        Oc(_TEST_OC_URL).qry("dual").run()
        Oc(make_eng(_TEST_OC_URL)).qry("dual").run()
    else:
        raise ValueError("Invalid db")


def test_db_meta_qry(test_tbl):
    db, df = test_tbl
    act = db.qry("test")
    exp = _Qry
    assert isinstance(act, exp)


def test_db_meta_list_tables(test_tbl):
    db, df = test_tbl
    if isinstance(db, Pg):
        sql = """
select
    table_catalog,table_schema,table_name
    ,is_insertable_into,commit_action
from information_schema.tables
where table_schema
not in ('information_schema','pg_catalog')
"""
        act = db.list_tables()
    elif isinstance(db, Lt):
        sql = """
select * from sqlite_master
where type ='table'
and name NOT LIKE 'sqlite_%'
"""
        act = db.list_tables()
    elif isinstance(db, Oc):
        sql = """
select/*+PARALLEL (4)*/ owner,table_name
    ,max(column_name),min(column_name)
from all_tab_columns
where owner = 'DWOPT_TEST'
group by owner,table_name
"""
        act = db.list_tables("dwopt_test")
    exp = db.run(sql)
    assert_frame_equal(act, exp)


def test_db_meta_table_cols(test_tbl):
    db, df = test_tbl
    if isinstance(db, Pg):
        sql = """
select column_name, data_type
from information_schema.columns
where table_schema = 'public'
and table_name = 'test'
"""
        act = db.table_cols("public.test")
    elif isinstance(db, Lt):
        return True
    elif isinstance(db, Oc):
        sql = """
select/*+PARALLEL (4)*/ *
from all_tab_columns
where owner = 'test_schema'
and table_name = 'test'
"""
        act = db.table_cols("test_schema.test")
    exp = db.run(sql)
    assert_frame_equal(act, exp)


def test_db_meta_table_sizes(test_tbl):
    db, df = test_tbl
    if isinstance(db, Pg):
        return True
    elif isinstance(db, Lt):
        return True
    elif isinstance(db, Oc):
        sql = """
select/*+PARALLEL (4)*/
    tablespace_name,segment_type,segment_name
    ,sum(bytes)/1024/1024 table_size_mb
from user_extents
group by tablespace_name,segment_type,segment_name
"""
    act = db.table_sizes()
    exp = db.run(sql)
    assert_frame_equal(act, exp)


def test_db_meta_list_cons(test_tbl):
    db, df = test_tbl
    if isinstance(db, Pg):
        sql = """
select * from information_schema.constraint_table_usage
"""
    elif isinstance(db, Lt):
        return True
    elif isinstance(db, Oc):
        return True
    act = db.list_cons()
    exp = db.run(sql)
    assert_frame_equal(act, exp)
