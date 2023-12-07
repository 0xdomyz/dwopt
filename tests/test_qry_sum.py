from pandas.testing import assert_frame_equal, assert_series_equal

from dwopt import Oc


def test_qry_sum_top(test_tbl):
    db, df = test_tbl
    act = db.qry("test").order_by("id").top()
    exp = db.run("select * from test where id = 0").iloc[0, :]
    assert_series_equal(act, exp)


def test_qry_sum_cols(test_tbl):
    db, df = test_tbl
    act = db.qry("test").cols()
    exp = df.columns.tolist()
    assert act == exp


def test_qry_sum_head(test_tbl):
    db, df = test_tbl
    act = db.qry("test").order_by("id").head()
    exp = db.run("select * from test where id < 5")
    assert_frame_equal(act, exp)


def test_qry_sum_len(test_tbl):
    db, df = test_tbl
    act = db.qry("test").len()
    exp = df.shape[0]
    assert act == exp


def test_qry_sum_dist(test_tbl):
    db, df = test_tbl
    act = db.qry("test").dist("id").iloc[0]
    exp = df.loc[:, "id"].nunique()
    assert act == exp


def test_qry_sum_mimx(test_tbl):
    db, df = test_tbl
    act = db.qry("test").mimx("amt").tolist()
    exp = [df.loc[:, "amt"].max(), df.loc[:, "amt"].min()]
    assert act == exp


def test_qry_sum_valc(test_tbl):
    db, df = test_tbl
    sql = """
select cat,count(1) as n
from test
group by cat
order by n desc
    """
    act = db.qry("test").valc("cat")
    exp = db.run(sql)
    assert_frame_equal(act, exp)


def test_qry_sum_hash(test_tbl):
    db, df = test_tbl
    sql = """
select
    ora_hash(sum(ora_hash(
        id || '_' || cat
    ) - 4294967296/2)) hash
from test
    """
    if isinstance(db, Oc):
        act = db.qry("test").hash("id", "cat")
        exp = db.run(sql).iloc[0, 0]
        assert act == exp
    else:
        assert True
        return
