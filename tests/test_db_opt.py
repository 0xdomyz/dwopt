from pandas.testing import assert_frame_equal
from dwopt import Pg, Lt, Oc
import pandas as pd
import datetime
import pytest


def assert_frame_equal_reset_index(a, b):
    assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True))


def test_db_opt_run(test_tbl):
    db, df = test_tbl

    if isinstance(db, Lt):
        exp = df.assign(
            date=lambda x: x.date.astype(str).where(~x.date.isna(), None),
            time=lambda x: x.time.astype(str).where(~x.time.isna(), None),
        ).loc[lambda x: x.id <= 9, :]
    elif isinstance(db, Pg):
        exp = df.loc[lambda x: x.id <= 9, :]
    elif isinstance(db, Oc):
        exp = df.loc[lambda x: x.id <= 9, :]
    else:
        raise ValueError

    act = db.run("select * from test where id <= 9 order by id")
    assert_frame_equal_reset_index(act, exp)

    act = db.run("select * from test where id <= :id order by id", args={"id": 9})
    assert_frame_equal_reset_index(act, exp)

    act = db.run("select * from test where id <= :id order by id", mods={"id": 9})
    assert_frame_equal_reset_index(act, exp)

    act = db.run("select * from test where id <= :id order by id", id=9)
    assert_frame_equal_reset_index(act, exp)


def test_db_opt_create(test_tbl, test_tbl2):
    db, df = test_tbl

    if isinstance(db, Pg):
        db.create(
            "test2",
            dtypes={
                "id": "bigint primary key",
                "score": "float8",
                "amt": "bigint",
                "cat": "varchar(20)",
            },
            date="date",
            time="timestamp",
        )
    elif isinstance(db, Lt):
        db.create(
            "test2",
            dtypes={
                "id": "integer primary key",
                "score": "real",
                "amt": "integer",
                "cat": "text",
            },
            date="text",
            time="text",
        )
    elif isinstance(db, Oc):
        db.create(
            "test2",
            dtypes={
                "id": "number primary key",
                "score": "float",
                "amt": "number",
                "cat": "varchar2(20)",
            },
            date="date",
            time="timestamp",
        )
    else:
        raise ValueError

    db.run("insert into test2 select * from test")
    act = db.run("select * from test2 order by id")
    exp = db.run("select * from test order by id")
    assert_frame_equal_reset_index(act, exp)


def test_db_opt_add_pkey(test_tbl, test_tbl2):
    db, df = test_tbl
    if isinstance(db, Pg):
        db.run("create table test2 as select * from test")
        db.add_pkey("test2", "id")
    elif isinstance(db, Lt):
        pass
    elif isinstance(db, Oc):
        pass
    else:
        raise ValueError


def test_db_opt_create_schema(test_tbl, test_tbl2):
    db, df = test_tbl
    if isinstance(db, Pg):
        try:
            db.run("drop schema test cascade")
        except Exception as ex:
            if "does not exist" in str(ex):
                pass
            else:
                raise (ex)
        db.create_schema("test")
        db.run("create table test.test (col int)")
        db.run("drop schema test cascade")
    elif isinstance(db, Lt):
        pass
    elif isinstance(db, Oc):
        pass
    else:
        raise ValueError


def test_db_opt_drop(test_tbl, test_tbl2):
    db, df = test_tbl
    db.run("create table test2 as select * from test")
    db.drop("test2")
    with pytest.raises(Exception) as e_info:
        db.run("select count(1) from test2")


def test_db_opt_write_nodup(test_tbl, test_tbl2):
    db, df = test_tbl
    db.run("create table test2 as select * from test where 1=2")
    if isinstance(db, Pg):
        db.write(df, "test2")
        db.write_nodup(df, "test2", ["id"])
        tbl = db.run("select * from test2 order by id")
    elif isinstance(db, Lt):
        db.write(
            df.assign(time=lambda x: x.time.astype(str).where(~x.time.isna(), None)),
            "test2",
        )
        db.write_nodup(
            df.assign(time=lambda x: x.time.astype(str).where(~x.time.isna(), None)),
            "test2",
            ["id"],
        )
        tbl = (
            db.qry("test2")
            .run()
            .assign(
                date=lambda x: x["date"].apply(
                    lambda x: datetime.date.fromisoformat(x) if x else None
                ),
                time=lambda x: pd.to_datetime(x.time),
            )
        )
    elif isinstance(db, Oc):
        db.write(
            df,
            "test2",
        )
        db.write_nodup(
            df,
            "test2",
            ["id"],
        )
        tbl = db.run("select * from test2 order by id")
    else:
        raise ValueError
    assert_frame_equal_reset_index(tbl, df)


def test_db_opt_cwrite(test_tbl, test_tbl2):
    db, df = test_tbl
    if isinstance(db, Pg):
        db.cwrite(df, "test2")
        tbl = db.run("select * from test2 order by id").assign(
            date=lambda x: x["date"].apply(
                lambda x: datetime.date.fromisoformat(x) if x else None
            )
        )
    elif isinstance(db, Lt):
        db.cwrite(
            df.assign(time=lambda x: x.time.astype(str).where(~x.time.isna(), None)),
            "test2",
        )
        tbl = (
            db.qry("test2")
            .run()
            .assign(
                date=lambda x: x["date"].apply(
                    lambda x: datetime.date.fromisoformat(x) if x else None
                ),
                time=lambda x: pd.to_datetime(x.time),
            )
        )
    elif isinstance(db, Oc):
        db.cwrite(
            df,
            "test2",
        )
        tbl = db.run("select * from test2 order by id").assign(
            date=lambda x: x["date"].apply(
                lambda x: datetime.datetime.strptime(x, "%d-%b-%y").date()
                if x
                else None
            )
        )
        df = df.assign(
            time=lambda x: x["time"].apply(lambda x: x.replace(microsecond=0))
        )
    else:
        raise ValueError
    assert_frame_equal_reset_index(tbl, df)
