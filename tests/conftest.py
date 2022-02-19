from dwopt import make_eng, Pg, Lt, Oc
import sqlalchemy as alc
import pandas as pd
import random
import pytest

@pytest.fixture(scope="session")
def fix_df():
    """Test dataframe"""
    n = 10000
    random.seed(0)
    df = pd.DataFrame(
        {
            "id": range(n)
            ,"score": [random.random() for i in range(n)]
            ,"amt": [random.choice(range(1000)) for i in range(n)]
            ,"cat": [random.choice(["test", "train"]) for i in range(n)]
            ,'time': [
                random.choice(["2013-01-02","2013-02-02","2013-03-02"])
                for i in range(n)
            ]
        }
    )
    return df

@pytest.fixture(scope = "session")
def fix_pg(fix_df):
    """
    Postgre test database object and test table creation.

    Test table
    ----------
    Database: dwopt_test
    Table: public.test

    Set up
    ------
    .. code-block:: console

        psql -U postgres
        CREATE DATABASE dwopt_test;
        CREATE USER dwopt_tester WITH PASSWORD '1234';
        GRANT ALL PRIVILEGES ON DATABASE dwopt_test to dwopt_tester;
    """
    df = fix_df
    url = "postgresql://dwopt_tester:1234@localhost/dwopt_test"
    engine = alc.create_engine(url)
    meta = alc.MetaData()
    test_tbl = alc.Table(
        'test', meta,
        alc.Column('id', alc.dialects.postgresql.BIGINT,
            primary_key=True),
        alc.Column('score', alc.Float),
        alc.Column('amt', alc.dialects.postgresql.BIGINT),
        alc.Column('cat', alc.String(20)),
        alc.Column('time', alc.String(20))
    )
    try:
        with engine.connect() as conn:
            conn.execute(test_tbl.delete())
    except Exception as ex:
        print(ex)
    meta.create_all(engine)
    with engine.connect() as conn:
        conn.execute(test_tbl.insert(), df.to_dict('records'))
    return Pg(engine)

@pytest.fixture(scope="session")
def fix_lt(fix_df):
    """
    Sqlite test database object and test table creation.
    
    Test table
    ----------
    Database: memory
    Table: test
    """
    df = fix_df
    url = "sqlite://"
    engine = alc.create_engine(url)
    meta = alc.MetaData()
    test_tbl = alc.Table(
        'test', meta,
        alc.Column('id', alc.Integer, primary_key=True),
        alc.Column('score', alc.REAL),
        alc.Column('amt', alc.Integer),
        alc.Column('cat', alc.String),
        alc.Column('time', alc.String)
    )
    meta.create_all(engine)
    with engine.connect() as conn:
        conn.execute(test_tbl.insert(), df.to_dict('records'))
    return Lt(engine)

@pytest.fixture(scope="session")
def fix_oc(fix_df):
    """
    Oracle test database object and test table creation.

    Test table
    ----------
    Database: dwopt_test
    Table: test_schema.test

    Set up
    ------
    Install oracle db from
    `link <https://www.oracle.com/database/technologies/xe-downloads.html>`.
    """
    pass


@pytest.fixture(scope = "session")
def db_df(request, fix_df, fix_lt):
    """Test sqlite only in github testing environment."""
    db = fix_lt
    return db,fix_df

#@pytest.fixture(scope = "session", params = ['pg','lt'])
#def db_df(request, fix_df, fix_pg, fix_lt):
#   """Test sqlite and postgre on local computer.
#
#      To-do: fix param that selects which to test.
#   """
#   if request.param == 'pg':
#       db = fix_pg
#   else:
#       db = fix_lt
#   return db,fix_df