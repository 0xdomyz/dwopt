import dwopt


def test_db_qry_qry(db_df):
    db, df = db_df
    act = db.qry("test")
    exp = dwopt._qry._Qry
    assert isinstance(act, exp)
