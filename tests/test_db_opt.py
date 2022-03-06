from pandas.testing import assert_frame_equal
from dwopt import Pg, Lt, Oc, make_test_tbl
import pandas as pd
import datetime

def test_db_opt_run(db_df):
    db, df = db_df
    db.run("select * from test limit 1")

    act = db.run("select * from test limit 1")
    exp = df.iloc[0:1, :]
    assert_frame_equal(act, exp)

    act = db.run(
        "select * from test where score > :score limit 2", args={"score": "0.9"}
    )
    exp = df.loc[lambda x: x.score > 0.9, :].reset_index(drop=True).iloc[0:2, :]
    assert_frame_equal(act, exp)

    act = db.run("select * from test where score > :score limit 2", score=0.9)
    assert_frame_equal(act, exp)


def test_db_opt_create(db_df):
    db, df = db_df

    if isinstance(db, Pg):
        db.drop("test2")
        db.create(
            tbl_nme="test2",
            dtypes={
                "id": "bigint",
                "score": "float8",
                "amt": "bigint",
                "cat": "varchar(20)",
                "time": "varchar(20)",
                "constraint df2_pk": "primary key (id)",
            },
        )
    elif isinstance(db, Lt):
        db.drop("test2")
        db.create(
            tbl_nme="test2",
            dtypes={
                "id": "integer",
                "score": "real",
                "amt": "integer",
                "cat": "text",
                "time": "text",
                "constraint df2_pk": "primary key (id)",
            },
        )
    elif isinstance(db, Oc):
        raise NotImplementedError

    act = db.run("select * from test2").columns.tolist()
    exp = db.run("select * from test").columns.tolist()
    assert act == exp


def test_db_opt_add_pkey(db_df):
    db, df = db_df
    if isinstance(db, Pg):
        db.drop("test2")
        db.run("create table test2 as select * from test")
        db.add_pkey("test2", "id")
    elif isinstance(db, Lt):
        pass
    elif isinstance(db, Oc):
        raise NotImplementedError


def test_db_opt_drop(db_df):
    db, df = db_df
    db.drop("test2")
    db.run("create table test2 as select * from test")
    db.drop("test2")


def test_db_opt_write(db_df):
    db, df = db_df
    db.drop("test2")
    db.run("create table test2 as select * from test where 1=2")
    db.write(df, "test2")
    act = db.run("select * from test order by id")
    exp = db.run("select * from test2 order by id")
    assert_frame_equal(act, exp)


def test_db_opt_write_reverse():
    lt, df = make_test_tbl('lt', 'test')
    tbl = lt.qry('test').run().assign(
        date = lambda x:x["date"].apply(lambda x:
            datetime.date.fromisoformat(x) if x else None
        ),
        time = lambda x:pd.to_datetime(x.time)
    )
    assert_frame_equal(tbl, df)


def test_db_opt_write_nodup(db_df):
    db, df = db_df
    db.drop("test2")
    db.run("create table test2 as select * from test where 1=2")
    db.write(df, "test2")
    db.write_nodup(df, "test2", ["id"])
    act = db.run("select * from test order by id")
    exp = db.run("select * from test2 order by id")
    assert_frame_equal(act, exp)
