from pandas.testing import assert_frame_equal, assert_series_equal
from dwopt import Oc

def test_qry_sum_top(db_df):
    db, df = db_df
    act = db.qry('test').top()
    exp = df.iloc[0,:]
    assert_series_equal(act,exp)

def test_qry_sum_cols(db_df):
    db, df = db_df
    act = db.qry('test').cols()
    exp = df.columns.tolist()
    assert act == exp

def test_qry_sum_head(db_df):
    db, df = db_df
    act = db.qry('test').head()
    exp = df.iloc[:5,:]
    assert_frame_equal(act,exp)


def test_qry_sum_len(db_df):
    db, df = db_df
    act = db.qry('test').len()
    exp = df.shape[0]
    assert act == exp


def test_qry_sum_dist(db_df):
    db, df = db_df
    act = db.qry('test').dist('id')[0]
    exp = df.loc[:,'id'].nunique()
    assert act == exp


def test_qry_sum_mimx(db_df):
    db, df = db_df
    act = db.qry('test').mimx('amt').tolist()
    exp = [df.loc[:,'amt'].max(), df.loc[:,'amt'].min()]
    assert act == exp


def test_qry_sum_valc(db_df):
    db, df = db_df
    sql = """
select cat,count(1) as n
from test
group by cat
order by n desc
    """
    act = db.qry('test').valc('cat')
    exp = db.run(sql)
    assert_frame_equal(act,exp)

def test_qry_sum_hash(db_df):
    db, df = db_df
    sql = """
select
    ora_hash(sum(ora_hash(
        id || '_' || cat
    ) - 4294967296/2)) hash
from test
    """
    if isinstance(db, Oc):
        act = db.qry('test').hash('id, cat')
        exp = db.run(sql)
        assert_series_equal(act,exp)
    else:
        assert 1 == 1

