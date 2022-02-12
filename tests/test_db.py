from pandas.testing import assert_frame_equal

def test_run(db_df):
    db, df = db_df
    db.run('select * from test limit 1')

    act = db.run('select * from test limit 1')
    exp = df.iloc[0:1,:]
    assert_frame_equal(act,exp)

    act = db.run('select * from test where score > :score limit 2'
        ,args = {'score':'0.9'})
    exp = (
        df.loc[lambda x:x.score > 0.9,:]
        .reset_index(drop=True).iloc[0:2,:]
    )
    assert_frame_equal(act,exp)

    act = db.run('select * from test where score > :score limit 2'
        ,score = 0.9)
    assert_frame_equal(act,exp)
