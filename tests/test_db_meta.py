from pandas.testing import assert_frame_equal
from dwopt import Pg, Lt, Oc
import dwopt


def test_db_meta_qry(test_tbl):
    db, df = test_tbl
    act = db.qry("test")
    exp = dwopt._qry._Qry
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
    elif isinstance(db, Lt):
        sql = """
select * from sqlite_master
where type ='table'
and name NOT LIKE 'sqlite_%'
"""
    elif isinstance(db, Oc):
        sql = """
select/*+PARALLEL (4)*/ owner,table_name
    ,max(column_name),min(column_name)
from all_tab_columns
where owner = 'test_schema'
group by owner,table_name
"""
    act = db.list_tables()
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
