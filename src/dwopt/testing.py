import random
import pandas as pd
import datetime
from dwopt import make_eng, Pg, Lt, Oc

_TESTING_PG_URL = "postgresql://dwopt_tester:1234@localhost/dwopt_test"
_TESTING_LT_URL = "sqlite://"
_TESTING_OC_URL = ""


def make_test_df(n = 10000):
    """Make a test dataframe with various data types and missing values.

    The ``id`` and the ``amt`` column do not have missing values, the other columns
    have missing values.

    Parameters
    ------------
    n: int
        Number of records.
    
    Returns
    --------
    pandas.DataFrame
    
    Examples
    ----------
    >> from dwopt.testing import make_test_df
    >> make_test_df(10000)
    """
    random.seed(0)
    df = pd.DataFrame(
        {
            "id": range(n),
            "score": [random.uniform(-1, 5) for i in range(n)],
            "amt": random.choices(range(1000), k=n),
            "cat": random.choices(["test", "train"], k=n),
            "date": [datetime.date.fromisoformat(i) for i in 
                random.choices(["2022-01-01", "2022-02-02", "2022-03-03"], k=n)],
            "time": [datetime.datetime.fromisoformat(i) for i in 
                random.choices([
                    "2022-01-01 00:19:02.011135",
                    "2022-02-02 23:00:00.000000",
                    "2022-03-03 10:19:35.071235"], k=n)
            ]
        }
    )

    for col in ['score','cat','date','time']:
        df.loc[random.choices(range(n), k=int(n/20)), col] = None

    return df


def make_test_tbl(db, tbl_nme, n=10000):
    """Make or remake a test table on database.

    Parameters
    ------------
    db: dwopt._Db, or str
        Dwopt database operator object. Or one of ``pg``, ``lt``, and ``oc``,
        indicating usage of pre-defined testing database engines.
    tbl_nme: str
        Test table name.
    n: int
        Number of records.
    
    Returns
    ----------
    (Dwopt._Db, pandas.DataFrame):
        Tuple of database operator used, and the test dataframe.

    Notes
    ------
    Pre-defined testing database engines:

    * ``pg``: ``postgresql://dwopt_tester:1234@localhost/dwopt_test``
    * ``lt``: ``sqlite://``
    * ``oc``: Not implemented.

    Examples
    ----------
    >>> from dwopt import lt, make_test_tbl
    >>> make_test_tbl(lt, 'test')

    >>> from dwopt import make_test_tbl
    >>> lt, _ = make_test_tbl('lt', 'test')
    >>> lt.qry('test').len()
    """
    if isinstance(db, str):
        if db == 'pg':
            db = Pg(make_eng(_TESTING_PG_URL))
        elif db == 'lt':
            db = Lt(make_eng(_TESTING_LT_URL))
        elif db == 'pg':
            db = Oc(make_eng(_TESTING_OC_URL))
        else:
            raise ValueError("Invalid db str, use one of 'pg', 'lt', or 'oc'")

    db.drop(tbl_nme)
    if isinstance(db, Pg):
        db.create(
            tbl_nme=tbl_nme,
            dtypes={
                "id": "bigint primary key",
                "score": "float8",
                "amt": "bigint",
                "cat": "varchar(20)",
                "date": "date",
                "time": "timestamp"
            },
        )
    elif isinstance(db, Lt):
        db.create(
            tbl_nme=tbl_nme,
            dtypes={
                "id": "integer primary key",
                "score": "real",
                "amt": "integer",
                "cat": "text",
                "date": "text",
                "time": "text"
            },
        )
    elif isinstance(db, Oc):
        raise NotImplementedError
    else:
        raise ValueError("Invalid db, either one of database operator objects, or "
            "one of 'pg', 'lt', or 'oc'")
    df = make_test_df(n)
    db.write(df, tbl_nme)
    return db, df