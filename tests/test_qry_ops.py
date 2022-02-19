from pandas.testing import assert_frame_equal, assert_series_equal
from contextlib import redirect_stdout
import io

sql = 'select count(1) from test'

def test_qry_ops_run(db_df):
    db, df = db_df
    act = db.qry(sql = sql).run()
    exp = db.run(sql)
    assert_frame_equal(act, exp)

def test_qry_ops_print(db_df):
    db, df = db_df
    with redirect_stdout(io.StringIO()) as f:
        db.qry(sql = sql).print()
    act = f.getvalue()
    exp = sql
    assert act.strip() == exp.strip()