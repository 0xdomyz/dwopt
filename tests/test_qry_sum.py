from pandas.testing import assert_frame_equal, assert_series_equal

def test_qry_sum_top(db_df):
    db, df = db_df
    act = db.qry('test').top()
    exp = df.iloc[0,:]
    assert_series_equal(act,exp)
