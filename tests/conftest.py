from dwopt import make_eng, Pg, Lt, Oc
import pandas as pd
import random
import pytest

@pytest.fixture(scope="session")
def fix_df():
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
    pg = Pg(make_eng(url))
    pg.drop('test')
    pg.create(
            tbl_nme = 'test'
            ,dtypes = {
                'id':'bigint'
                ,'score':'float8'
                ,'amt':'bigint'
                ,'cat':'varchar(20)'
                ,'time':'varchar(20)'
                ,'constraint df_pk':
                    'primary key (id)'
            }
        )
    pg.write(df,'test')
    return pg

@pytest.fixture(scope="session")
def fix_lt(fix_df):
    df = fix_df
    url = "sqlite://"
    lt = Lt(make_eng(url))
    lt.drop('test')
    lt.create(
            tbl_nme = 'test'
            ,dtypes = {
                'id':'integer'
                ,'score':'real'
                ,'amt':'integer'
                ,'cat':'text'
                ,'time':'text'
                ,'constraint df_pk':
                    'primary key (id)'
            }
        )
    lt.write(df,'test')
    return lt

#oracle
#https://www.oracle.com/database/technologies/xe-downloads.html

#@pytest.fixture(scope = "session", params = ['pg','lt'])
#def db_df(request, fix_df, fix_pg, fix_lt):
#    if request.param == 'pg':
#        db = fix_pg
#    else:
#        db = fix_lt
#    return db,fix_df

@pytest.fixture(scope = "session")
def db_df(request, fix_df, fix_lt):
    db = fix_lt
    return db,fix_df