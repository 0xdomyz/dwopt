from pandas.testing import assert_frame_equal, assert_series_equal
from dwopt import Pg, Lt, Oc

def test_db_meta_top(db_df):
    db, df = db_df
    if isinstance(db, Pg):
        test_top_sql = """
select
    table_catalog,table_schema,table_name
    ,is_insertable_into,commit_action
from information_schema.tables
where table_schema
not in ('information_schema','pg_catalog')
"""
    elif isinstance(db, Lt):
        test_top_sql = """
select * from sqlite_schema
where type ='table'
and name NOT LIKE 'sqlite_%'
"""
    elif isinstance(db, Oc):
        test_top_sql = """
select/*+PARALLEL (4)*/ owner,table_name
    ,max(column_name),min(column_name)
from all_tab_columns
where owner = 'test_schema'
group by owner,table_name
"""
    act = db.list_tables()
    exp = db.run(test_top_sql)
    assert_frame_equal(act,exp)
