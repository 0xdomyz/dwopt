from pandas.testing import assert_frame_equal, assert_series_equal, assert_index_equal

def test_qry_cls_select(db_df):
    db, df = db_df

    act = db.qry('test').select('id, score, amt').run().columns
    exp = df.loc[:,['id','score','amt']].columns
    assert_index_equal(act,exp)

    act = db.qry('test').select('id', 'score', 'amt').run().columns
    assert_index_equal(act,exp)

    act = db.qry('test').select(['id', 'score', 'amt']).run().columns
    assert_index_equal(act,exp)
