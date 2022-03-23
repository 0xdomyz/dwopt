import random
import pandas as pd
import datetime
import dwopt
from dwopt.set_up import _TEST_PG_URL, _TEST_LT_URL, _TEST_OC_URL
import sqlalchemy as alc


def _make_test_df(n=10000):
    random.seed(0)
    df = pd.DataFrame(
        {
            "id": range(n),
            "score": [random.uniform(-1, 5) for i in range(n)],
            "amt": random.choices(range(1000), k=n),
            "cat": random.choices(["test", "train"], k=n),
            "dte": [
                datetime.date.fromisoformat(i)
                for i in random.choices(["2022-01-01", "2022-02-02", "2022-03-03"], k=n)
            ],
            "time": [
                datetime.datetime.fromisoformat(i)
                for i in random.choices(
                    [
                        "2022-01-01 00:19:02.011135",
                        "2022-02-02 23:00:00.000000",
                        "2022-03-03 10:19:35.071235",
                    ],
                    k=n,
                )
            ],
        }
    )

    for col in ["score", "cat", "dte", "time"]:
        df.loc[random.choices(range(n), k=int(n / 20)), col] = None

    return df


def _parse_sch_tbl_nme(sch_tbl_nme):
    db = dwopt.dbo._Db
    return db._parse_sch_tbl_nme(db, sch_tbl_nme)


def _make_pg_tbl(df, eng, sch_tbl_nme):
    _, sch, tbl_nme = _parse_sch_tbl_nme(sch_tbl_nme)
    meta = alc.MetaData()
    test_tbl = alc.Table(
        tbl_nme,
        meta,
        alc.Column("id", alc.dialects.postgresql.BIGINT, primary_key=True),
        alc.Column("score", alc.Float(8)),
        alc.Column("amt", alc.dialects.postgresql.BIGINT),
        alc.Column("cat", alc.String(20)),
        alc.Column("dte", alc.Date),
        alc.Column("time", alc.DateTime),
        schema=sch,
    )
    with eng.connect() as conn:
        test_tbl.drop(conn, checkfirst=True)
    meta.create_all(eng)
    with eng.connect() as conn:
        conn.execute(
            test_tbl.insert(),
            df.assign(
                time=lambda x: x.time.astype(object).where(~x.time.isna(), None)
            ).to_dict("records"),
        )


def _make_lt_tbl(df, eng, sch_tbl_nme):
    _, sch, tbl_nme = _parse_sch_tbl_nme(sch_tbl_nme)
    meta = alc.MetaData()
    test_tbl = alc.Table(
        tbl_nme,
        meta,
        alc.Column("id", alc.Integer, primary_key=True),
        alc.Column("score", alc.REAL),
        alc.Column("amt", alc.Integer),
        alc.Column("cat", alc.String),
        alc.Column("dte", alc.String),
        alc.Column("time", alc.String),
        schema=sch,
    )
    with eng.connect() as conn:
        test_tbl.drop(conn, checkfirst=True)
    meta.create_all(eng)
    with eng.connect() as conn:
        conn.execute(
            test_tbl.insert(),
            df.assign(
                time=lambda x: x.time.astype(str).where(~x.time.isna(), None)
            ).to_dict("records"),
        )


def _make_oc_tbl(df, eng, sch_tbl_nme):
    _, sch, tbl_nme = _parse_sch_tbl_nme(sch_tbl_nme)
    meta = alc.MetaData()
    test_tbl = alc.Table(
        tbl_nme,
        meta,
        alc.Column("id", alc.dialects.oracle.NUMBER, primary_key=True),
        alc.Column("score", alc.Float),
        alc.Column("amt", alc.dialects.oracle.NUMBER),
        alc.Column("cat", alc.String(20)),
        alc.Column("dte", alc.Date),
        alc.Column("time", alc.Date),
        schema=sch,
    )
    with eng.connect() as conn:
        test_tbl.drop(conn, checkfirst=True)
    meta.create_all(eng)
    with eng.connect() as conn:
        conn.execute(
            test_tbl.insert(),
            df.assign(
                score=lambda x: x.score.astype(object).where(~x.score.isna(), None),
                time=lambda x: x.time.astype(object).where(~x.time.isna(), None),
            ).to_dict("records"),
        )


def make_test_tbl(db, sch_tbl_nme="test", n=10000):
    """Make or remake a test table on database.

    Uses Sqlalchemy toolkits for table drop, creation, insertion.

    Parameters
    ------------
    db: dwopt.dbo._Db, or str
        Dwopt database operator object. Or one of ``'pg'``, ``'lt'``, and ``'oc'``,
        indicating usage of pre-defined testing database engines.
    sch_tbl_nme: str
        Table name in form ``my_schema1.my_table1`` or ``my_table1``.
    n: int
        Number of records.

    Returns
    ----------
    (dwopt.dbo._Db, pandas.DataFrame):
        Tuple of database operator used, and the test dataframe.

    Notes
    ------

    **Table specifications**

    ====== ============== ============== ==== ===================
    Column Column type    Object type    None Example
    ====== ============== ============== ==== ===================
    id     int64          int64               0
    score  float64        float64        NaN  4.066531
    amt    int64          int64               867
    cat    object         str            None train
    dte    object         datetime.date  None 2022-03-03
    time   datetime64[ns] datetime64[ns] NaT  2022-02-02 23:00:00
    ====== ============== ============== ==== ===================

    **Test database table specifications**

    ====== =========== ======= ============
    Column Postgre     Sqlite  Oracle
    ====== =========== ======= ============
    id     bigint      integer number
    score  float8      real    float
    amt    bigint      integer number
    cat    varchar(20) text    varchar2(20)
    dte    date        text    date
    time   timestamp   text    date
    ====== =========== ======= ============

    These datatypes are implemented via closest
    `Sqlalchemy datatypes <https://docs.sqlalchemy.org/en/14/core/type_basics.html>`_.

    The ``id`` column is made primary key in the test database tables.

    *Floating point types*

    The ``score`` column's ``NaN`` objects are converted into ``None`` before insertion
    for oracle.

    *Datetime types*

    The ``time`` column's ``NaT`` objects are converted into ``None`` before insertion
    for Postgre and Oracle.
    The ``time`` column are converted into str and None before insertion for Sqlite.

    **Pre-defined testing database engines**

    * ``pg``: ``postgresql://dwopt_tester:1234@localhost/dwopt_test``
    * ``lt``: ``sqlite://``
    * ``oc``: ``oracle://dwopt_test:1234@localhost:1521/?service_name=XEPDB1
      &encoding=UTF-8&nencoding=UTF-8``.

    **Install testing databases**

    *Postgre*::

        psql -U postgres
        CREATE DATABASE dwopt_test;
        CREATE USER dwopt_tester WITH PASSWORD '1234';
        GRANT ALL PRIVILEGES ON DATABASE dwopt_test to dwopt_tester;

    *Oracle*

    * Install oracle db from the
      `oracle xe <https://www.oracle.com/database/technologies/xe-downloads.html>`_.
    * Set ORACLE_HOME environment variable to point to installation location.
    * command::

        cd /d %ORACLE_HOME%\\bin
        sqlplus sys/[password]]@//localhost:1521/XEPDB1 as sysdba
        create user dwopt_test identified by 1234;
        grant create session to dwopt_test;
        grant create table to dwopt_test;
        grant unlimited tablespace to dwopt_test;

    Examples
    ----------
    Make test table via provided database operator:

    >>> from dwopt import lt, make_test_tbl
    >>> _ = make_test_tbl(lt)
    >>> lt.qry('test').len()
    10000

    Make database operator for the pre-defined test databases,
    then make test table on it:

    >>> from dwopt import make_test_tbl
    >>> lt, df = make_test_tbl('lt', 'foo', 999)
    >>> lt.eng
    Engine(sqlite://)
    >>> lt.qry('foo').len()
    999
    >>> len(df)
    999
    """
    if isinstance(db, str):
        if db == "pg":
            db = dwopt.db(_TEST_PG_URL)
        elif db == "lt":
            db = dwopt.db(_TEST_LT_URL)
        elif db == "oc":
            db = dwopt.db(_TEST_OC_URL)
        else:
            raise ValueError("Invalid db str, use one of 'pg', 'lt', or 'oc'")
    df = _make_test_df(n)
    dlc = db._dialect
    if dlc == "pg":
        _make_pg_tbl(df, db.eng, sch_tbl_nme)
    elif dlc == "lt":
        _make_lt_tbl(df, db.eng, sch_tbl_nme)
    elif dlc == "oc":
        _make_oc_tbl(df, db.eng, sch_tbl_nme)
    else:
        raise ValueError(
            "Invalid db, must be a database operator object, instances of "
            "(dwopt.Pg, dwopt.Lt, dwopt.Oc)"
        )
    return db, df
