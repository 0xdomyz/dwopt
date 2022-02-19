from pandas.testing import assert_frame_equal, assert_series_equal

test_top_sql = """
select * from sqlite_schema
where type ='table'
and name NOT LIKE 'sqlite_%'
"""

def test_db_meta_top(db_df):
    db, df = db_df
    act = db.list_tables()
    exp = db.run(test_top_sql)
    assert_frame_equal(act,exp)
