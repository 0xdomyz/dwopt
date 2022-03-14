import pandas as pd
import numpy as np
import sqlalchemy as alc

# db method


def list_tables(self, owner):
    """
    List all tables on database or specified schema.

    Parameters
    ----------
    owner : str
        Only applicable for oracle. Name of the schema.

    Returns
    -------
    pandas.DataFrame

    Notes
    -----

    Postgre sql used, `information_schema.tables
    <https://www.postgresql.org/docs/current/infoschema-tables.html>`_:

    .. code-block:: sql

        select
            table_catalog,table_schema,table_name
            ,is_insertable_into,commit_action
        from information_schema.tables
        where table_schema
        not in ('information_schema','pg_catalog')

    Sqlite sql used, `sqlite_schema <https://www.sqlite.org/schematab.html>`_:

    .. code-block:: sql

        select * from sqlite_master
        where type ='table'
        and name NOT LIKE 'sqlite_%'

    Oracle sql used, `all_tab_columns
    <https://docs.oracle.com/en/database/oracle/oracle-database/21/
    refrn/ALL_TAB_COLUMNS.html>`_:

    .. code-block:: sql

        select/*+PARALLEL (4)*/ owner,table_name
            ,max(column_name),min(column_name)
        from all_tab_columns
        where owner = ':owner'
        group by owner,table_name

    """
    raise NotImplementedError


def table_sizes(self):
    """
    List sizes of all tables in current schema.

    Returns
    -------
    pandas.DataFrame

    Notes
    -----

    Oracle sql used, `user_extents
    <https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/
    USER_EXTENTS.html>`_:

    .. code-block:: sql

        select/*+PARALLEL (4)*/
            tablespace_name,segment_type,segment_name
            ,sum(bytes)/1024/1024 table_size_mb
        from user_extents
        group by tablespace_name,segment_type,segment_name

    """
    raise NotImplementedError


def table_cols(self, sch_tbl_nme):
    """
    Show information of specified table's columns.

    Notes
    -----

    Postgre sql used, `information_schema.columns
    <https://www.postgresql.org/docs/current/infoschema-columns.html>`_:

    .. code-block:: sql

        select column_name, data_type
        from information_schema.columns
        where table_schema = ':schema_nme'
        and table_name = ':tbl_nme'

    Oracle sql used, `all_tab_columns
    <https://docs.oracle.com/en/database/oracle/oracle-database/21/
    refrn/ALL_TAB_COLUMNS.html>`_:

    .. code-block:: sql

        select/*+PARALLEL (4)*/ *
        from all_tab_columns
        where owner = ':schema_nme'
        and table_name = ':tbl_nme'

    Parameters
    ----------
    sch_tbl_nme : str
        Table name in format: `schema.table`.

    Returns
    -------
    pandas.DataFrame
    """
    raise NotImplementedError


def list_cons(self):
    """
    List all constraints.

    Returns
    -------
    pandas.DataFrame

    Notes
    -----

    Postgre sql used, `information_schema.constraint_table_usage
    <https://www.postgresql.org/docs/current/infoschema-
    constraint-table-usage.html>`_:

    .. code-block:: sql

        select * from information_schema.constraint_table_usage

    """
    raise NotImplementedError


def _guess_dtype(self, dtype):
    """See :meth:`dwopt.db._Db.create`"""
    if np.issubdtype(dtype, np.int64):
        return alc.Integer
    elif np.issubdtype(dtype, np.float64):
        return alc.Float
    elif np.issubdtype(dtype, np.datetime64):
        return alc.DateTime
    else:
        return alc.String


def _make_iris_df():
    res = [
        {"v1": 5.1, "v2": 3.5, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.9, "v2": 3.0, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.7, "v2": 3.2, "v3": 1.3, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.6, "v2": 3.1, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.6, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.4, "v2": 3.9, "v3": 1.7, "v4": 0.4, "v5": "setosa"},
        {"v1": 4.6, "v2": 3.4, "v3": 1.4, "v4": 0.3, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.4, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.4, "v2": 2.9, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.9, "v2": 3.1, "v3": 1.5, "v4": 0.1, "v5": "setosa"},
        {"v1": 5.4, "v2": 3.7, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.8, "v2": 3.4, "v3": 1.6, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.8, "v2": 3.0, "v3": 1.4, "v4": 0.1, "v5": "setosa"},
        {"v1": 4.3, "v2": 3.0, "v3": 1.1, "v4": 0.1, "v5": "setosa"},
        {"v1": 5.8, "v2": 4.0, "v3": 1.2, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.7, "v2": 4.4, "v3": 1.5, "v4": 0.4, "v5": "setosa"},
        {"v1": 5.4, "v2": 3.9, "v3": 1.3, "v4": 0.4, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.5, "v3": 1.4, "v4": 0.3, "v5": "setosa"},
        {"v1": 5.7, "v2": 3.8, "v3": 1.7, "v4": 0.3, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.8, "v3": 1.5, "v4": 0.3, "v5": "setosa"},
        {"v1": 5.4, "v2": 3.4, "v3": 1.7, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.7, "v3": 1.5, "v4": 0.4, "v5": "setosa"},
        {"v1": 4.6, "v2": 3.6, "v3": 1.0, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.3, "v3": 1.7, "v4": 0.5, "v5": "setosa"},
        {"v1": 4.8, "v2": 3.4, "v3": 1.9, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.0, "v3": 1.6, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.4, "v3": 1.6, "v4": 0.4, "v5": "setosa"},
        {"v1": 5.2, "v2": 3.5, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.2, "v2": 3.4, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.7, "v2": 3.2, "v3": 1.6, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.8, "v2": 3.1, "v3": 1.6, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.4, "v2": 3.4, "v3": 1.5, "v4": 0.4, "v5": "setosa"},
        {"v1": 5.2, "v2": 4.1, "v3": 1.5, "v4": 0.1, "v5": "setosa"},
        {"v1": 5.5, "v2": 4.2, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.9, "v2": 3.1, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.2, "v3": 1.2, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.5, "v2": 3.5, "v3": 1.3, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.9, "v2": 3.6, "v3": 1.4, "v4": 0.1, "v5": "setosa"},
        {"v1": 4.4, "v2": 3.0, "v3": 1.3, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.4, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.5, "v3": 1.3, "v4": 0.3, "v5": "setosa"},
        {"v1": 4.5, "v2": 2.3, "v3": 1.3, "v4": 0.3, "v5": "setosa"},
        {"v1": 4.4, "v2": 3.2, "v3": 1.3, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.5, "v3": 1.6, "v4": 0.6, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.8, "v3": 1.9, "v4": 0.4, "v5": "setosa"},
        {"v1": 4.8, "v2": 3.0, "v3": 1.4, "v4": 0.3, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.8, "v3": 1.6, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.6, "v2": 3.2, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.3, "v2": 3.7, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.3, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 7.0, "v2": 3.2, "v3": 4.7, "v4": 1.4, "v5": "sicolor"},
        {"v1": 6.4, "v2": 3.2, "v3": 4.5, "v4": 1.5, "v5": "sicolor"},
        {"v1": 6.9, "v2": 3.1, "v3": 4.9, "v4": 1.5, "v5": "sicolor"},
        {"v1": 5.5, "v2": 2.3, "v3": 4.0, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.5, "v2": 2.8, "v3": 4.6, "v4": 1.5, "v5": "sicolor"},
        {"v1": 5.7, "v2": 2.8, "v3": 4.5, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.3, "v2": 3.3, "v3": 4.7, "v4": 1.6, "v5": "sicolor"},
        {"v1": 4.9, "v2": 2.4, "v3": 3.3, "v4": 1.0, "v5": "sicolor"},
        {"v1": 6.6, "v2": 2.9, "v3": 4.6, "v4": 1.3, "v5": "sicolor"},
        {"v1": 5.2, "v2": 2.7, "v3": 3.9, "v4": 1.4, "v5": "sicolor"},
        {"v1": 5.0, "v2": 2.0, "v3": 3.5, "v4": 1.0, "v5": "sicolor"},
        {"v1": 5.9, "v2": 3.0, "v3": 4.2, "v4": 1.5, "v5": "sicolor"},
        {"v1": 6.0, "v2": 2.2, "v3": 4.0, "v4": 1.0, "v5": "sicolor"},
        {"v1": 6.1, "v2": 2.9, "v3": 4.7, "v4": 1.4, "v5": "sicolor"},
        {"v1": 5.6, "v2": 2.9, "v3": 3.6, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.7, "v2": 3.1, "v3": 4.4, "v4": 1.4, "v5": "sicolor"},
        {"v1": 5.6, "v2": 3.0, "v3": 4.5, "v4": 1.5, "v5": "sicolor"},
        {"v1": 5.8, "v2": 2.7, "v3": 4.1, "v4": 1.0, "v5": "sicolor"},
        {"v1": 6.2, "v2": 2.2, "v3": 4.5, "v4": 1.5, "v5": "sicolor"},
        {"v1": 5.6, "v2": 2.5, "v3": 3.9, "v4": 1.1, "v5": "sicolor"},
        {"v1": 5.9, "v2": 3.2, "v3": 4.8, "v4": 1.8, "v5": "sicolor"},
        {"v1": 6.1, "v2": 2.8, "v3": 4.0, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.3, "v2": 2.5, "v3": 4.9, "v4": 1.5, "v5": "sicolor"},
        {"v1": 6.1, "v2": 2.8, "v3": 4.7, "v4": 1.2, "v5": "sicolor"},
        {"v1": 6.4, "v2": 2.9, "v3": 4.3, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.6, "v2": 3.0, "v3": 4.4, "v4": 1.4, "v5": "sicolor"},
        {"v1": 6.8, "v2": 2.8, "v3": 4.8, "v4": 1.4, "v5": "sicolor"},
        {"v1": 6.7, "v2": 3.0, "v3": 5.0, "v4": 1.7, "v5": "sicolor"},
        {"v1": 6.0, "v2": 2.9, "v3": 4.5, "v4": 1.5, "v5": "sicolor"},
        {"v1": 5.7, "v2": 2.6, "v3": 3.5, "v4": 1.0, "v5": "sicolor"},
        {"v1": 5.5, "v2": 2.4, "v3": 3.8, "v4": 1.1, "v5": "sicolor"},
        {"v1": 5.5, "v2": 2.4, "v3": 3.7, "v4": 1.0, "v5": "sicolor"},
        {"v1": 5.8, "v2": 2.7, "v3": 3.9, "v4": 1.2, "v5": "sicolor"},
        {"v1": 6.0, "v2": 2.7, "v3": 5.1, "v4": 1.6, "v5": "sicolor"},
        {"v1": 5.4, "v2": 3.0, "v3": 4.5, "v4": 1.5, "v5": "sicolor"},
        {"v1": 6.0, "v2": 3.4, "v3": 4.5, "v4": 1.6, "v5": "sicolor"},
        {"v1": 6.7, "v2": 3.1, "v3": 4.7, "v4": 1.5, "v5": "sicolor"},
        {"v1": 6.3, "v2": 2.3, "v3": 4.4, "v4": 1.3, "v5": "sicolor"},
        {"v1": 5.6, "v2": 3.0, "v3": 4.1, "v4": 1.3, "v5": "sicolor"},
        {"v1": 5.5, "v2": 2.5, "v3": 4.0, "v4": 1.3, "v5": "sicolor"},
        {"v1": 5.5, "v2": 2.6, "v3": 4.4, "v4": 1.2, "v5": "sicolor"},
        {"v1": 6.1, "v2": 3.0, "v3": 4.6, "v4": 1.4, "v5": "sicolor"},
        {"v1": 5.8, "v2": 2.6, "v3": 4.0, "v4": 1.2, "v5": "sicolor"},
        {"v1": 5.0, "v2": 2.3, "v3": 3.3, "v4": 1.0, "v5": "sicolor"},
        {"v1": 5.6, "v2": 2.7, "v3": 4.2, "v4": 1.3, "v5": "sicolor"},
        {"v1": 5.7, "v2": 3.0, "v3": 4.2, "v4": 1.2, "v5": "sicolor"},
        {"v1": 5.7, "v2": 2.9, "v3": 4.2, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.2, "v2": 2.9, "v3": 4.3, "v4": 1.3, "v5": "sicolor"},
        {"v1": 5.1, "v2": 2.5, "v3": 3.0, "v4": 1.1, "v5": "sicolor"},
        {"v1": 5.7, "v2": 2.8, "v3": 4.1, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.3, "v2": 3.3, "v3": 6.0, "v4": 2.5, "v5": "rginica"},
        {"v1": 5.8, "v2": 2.7, "v3": 5.1, "v4": 1.9, "v5": "rginica"},
        {"v1": 7.1, "v2": 3.0, "v3": 5.9, "v4": 2.1, "v5": "rginica"},
        {"v1": 6.3, "v2": 2.9, "v3": 5.6, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.5, "v2": 3.0, "v3": 5.8, "v4": 2.2, "v5": "rginica"},
        {"v1": 7.6, "v2": 3.0, "v3": 6.6, "v4": 2.1, "v5": "rginica"},
        {"v1": 4.9, "v2": 2.5, "v3": 4.5, "v4": 1.7, "v5": "rginica"},
        {"v1": 7.3, "v2": 2.9, "v3": 6.3, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.7, "v2": 2.5, "v3": 5.8, "v4": 1.8, "v5": "rginica"},
        {"v1": 7.2, "v2": 3.6, "v3": 6.1, "v4": 2.5, "v5": "rginica"},
        {"v1": 6.5, "v2": 3.2, "v3": 5.1, "v4": 2.0, "v5": "rginica"},
        {"v1": 6.4, "v2": 2.7, "v3": 5.3, "v4": 1.9, "v5": "rginica"},
        {"v1": 6.8, "v2": 3.0, "v3": 5.5, "v4": 2.1, "v5": "rginica"},
        {"v1": 5.7, "v2": 2.5, "v3": 5.0, "v4": 2.0, "v5": "rginica"},
        {"v1": 5.8, "v2": 2.8, "v3": 5.1, "v4": 2.4, "v5": "rginica"},
        {"v1": 6.4, "v2": 3.2, "v3": 5.3, "v4": 2.3, "v5": "rginica"},
        {"v1": 6.5, "v2": 3.0, "v3": 5.5, "v4": 1.8, "v5": "rginica"},
        {"v1": 7.7, "v2": 3.8, "v3": 6.7, "v4": 2.2, "v5": "rginica"},
        {"v1": 7.7, "v2": 2.6, "v3": 6.9, "v4": 2.3, "v5": "rginica"},
        {"v1": 6.0, "v2": 2.2, "v3": 5.0, "v4": 1.5, "v5": "rginica"},
        {"v1": 6.9, "v2": 3.2, "v3": 5.7, "v4": 2.3, "v5": "rginica"},
        {"v1": 5.6, "v2": 2.8, "v3": 4.9, "v4": 2.0, "v5": "rginica"},
        {"v1": 7.7, "v2": 2.8, "v3": 6.7, "v4": 2.0, "v5": "rginica"},
        {"v1": 6.3, "v2": 2.7, "v3": 4.9, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.7, "v2": 3.3, "v3": 5.7, "v4": 2.1, "v5": "rginica"},
        {"v1": 7.2, "v2": 3.2, "v3": 6.0, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.2, "v2": 2.8, "v3": 4.8, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.1, "v2": 3.0, "v3": 4.9, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.4, "v2": 2.8, "v3": 5.6, "v4": 2.1, "v5": "rginica"},
        {"v1": 7.2, "v2": 3.0, "v3": 5.8, "v4": 1.6, "v5": "rginica"},
        {"v1": 7.4, "v2": 2.8, "v3": 6.1, "v4": 1.9, "v5": "rginica"},
        {"v1": 7.9, "v2": 3.8, "v3": 6.4, "v4": 2.0, "v5": "rginica"},
        {"v1": 6.4, "v2": 2.8, "v3": 5.6, "v4": 2.2, "v5": "rginica"},
        {"v1": 6.3, "v2": 2.8, "v3": 5.1, "v4": 1.5, "v5": "rginica"},
        {"v1": 6.1, "v2": 2.6, "v3": 5.6, "v4": 1.4, "v5": "rginica"},
        {"v1": 7.7, "v2": 3.0, "v3": 6.1, "v4": 2.3, "v5": "rginica"},
        {"v1": 6.3, "v2": 3.4, "v3": 5.6, "v4": 2.4, "v5": "rginica"},
        {"v1": 6.4, "v2": 3.1, "v3": 5.5, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.0, "v2": 3.0, "v3": 4.8, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.9, "v2": 3.1, "v3": 5.4, "v4": 2.1, "v5": "rginica"},
        {"v1": 6.7, "v2": 3.1, "v3": 5.6, "v4": 2.4, "v5": "rginica"},
        {"v1": 6.9, "v2": 3.1, "v3": 5.1, "v4": 2.3, "v5": "rginica"},
        {"v1": 5.8, "v2": 2.7, "v3": 5.1, "v4": 1.9, "v5": "rginica"},
        {"v1": 6.8, "v2": 3.2, "v3": 5.9, "v4": 2.3, "v5": "rginica"},
        {"v1": 6.7, "v2": 3.3, "v3": 5.7, "v4": 2.5, "v5": "rginica"},
        {"v1": 6.7, "v2": 3.0, "v3": 5.2, "v4": 2.3, "v5": "rginica"},
        {"v1": 6.3, "v2": 2.5, "v3": 5.0, "v4": 1.9, "v5": "rginica"},
        {"v1": 6.5, "v2": 3.0, "v3": 5.2, "v4": 2.0, "v5": "rginica"},
        {"v1": 6.2, "v2": 3.4, "v3": 5.4, "v4": 2.3, "v5": "rginica"},
        {"v1": 5.9, "v2": 3.0, "v3": 5.1, "v4": 1.8, "v5": "rginica"},
    ]
    res = pd.DataFrame(res)
    res.columns = [
        "Sepal.Length",
        "Sepal.Width",
        "Petal.Length",
        "Petal.Width",
        "Species",
    ]
    return res


def _make_mtcars_df():
    res1 = [
        {"v0": "Mazda RX4"},
        {"v0": "Mazda RX4 Wag"},
        {"v0": "Datsun 710"},
        {"v0": "Hornet 4 Drive"},
        {"v0": "Hornet Sportabout"},
        {"v0": "Valiant"},
        {"v0": "Duster 360"},
        {"v0": "Merc 240D"},
        {"v0": "Merc 230"},
        {"v0": "Merc 280"},
        {"v0": "Merc 280C"},
        {"v0": "Merc 450SE"},
        {"v0": "Merc 450SL"},
        {"v0": "Merc 450SLC"},
        {"v0": "Cadillac Fleetwood"},
        {"v0": "Lincoln Continental"},
        {"v0": "Chrysler Imperial"},
        {"v0": "Fiat 128"},
        {"v0": "Honda Civic"},
        {"v0": "Toyota Corolla"},
        {"v0": "Toyota Corona"},
        {"v0": "Dodge Challenger"},
        {"v0": "AMC Javelin"},
        {"v0": "Camaro Z28"},
        {"v0": "Pontiac Firebird"},
        {"v0": "Fiat X1-9"},
        {"v0": "Porsche 914-2"},
        {"v0": "Lotus Europa"},
        {"v0": "Ford Pantera L"},
        {"v0": "Ferrari Dino"},
        {"v0": "Maserati Bora"},
        {"v0": "Volvo 142E"},
    ]
    res2 = [
        {"v1": 21.0, "v2": 6, "v3": 160.0, "v4": 110},
        {"v1": 21.0, "v2": 6, "v3": 160.0, "v4": 110},
        {"v1": 22.8, "v2": 4, "v3": 108.0, "v4": 93},
        {"v1": 21.4, "v2": 6, "v3": 258.0, "v4": 110},
        {"v1": 18.7, "v2": 8, "v3": 360.0, "v4": 175},
        {"v1": 18.1, "v2": 6, "v3": 225.0, "v4": 105},
        {"v1": 14.3, "v2": 8, "v3": 360.0, "v4": 245},
        {"v1": 24.4, "v2": 4, "v3": 146.7, "v4": 62},
        {"v1": 22.8, "v2": 4, "v3": 140.8, "v4": 95},
        {"v1": 19.2, "v2": 6, "v3": 167.6, "v4": 123},
        {"v1": 17.8, "v2": 6, "v3": 167.6, "v4": 123},
        {"v1": 16.4, "v2": 8, "v3": 275.8, "v4": 180},
        {"v1": 17.3, "v2": 8, "v3": 275.8, "v4": 180},
        {"v1": 15.2, "v2": 8, "v3": 275.8, "v4": 180},
        {"v1": 10.4, "v2": 8, "v3": 472.0, "v4": 205},
        {"v1": 10.4, "v2": 8, "v3": 460.0, "v4": 215},
        {"v1": 14.7, "v2": 8, "v3": 440.0, "v4": 230},
        {"v1": 32.4, "v2": 4, "v3": 78.7, "v4": 66},
        {"v1": 30.4, "v2": 4, "v3": 75.7, "v4": 52},
        {"v1": 33.9, "v2": 4, "v3": 71.1, "v4": 65},
        {"v1": 21.5, "v2": 4, "v3": 120.1, "v4": 97},
        {"v1": 15.5, "v2": 8, "v3": 318.0, "v4": 150},
        {"v1": 15.2, "v2": 8, "v3": 304.0, "v4": 150},
        {"v1": 13.3, "v2": 8, "v3": 350.0, "v4": 245},
        {"v1": 19.2, "v2": 8, "v3": 400.0, "v4": 175},
        {"v1": 27.3, "v2": 4, "v3": 79.0, "v4": 66},
        {"v1": 26.0, "v2": 4, "v3": 120.3, "v4": 91},
        {"v1": 30.4, "v2": 4, "v3": 95.1, "v4": 113},
        {"v1": 15.8, "v2": 8, "v3": 351.0, "v4": 264},
        {"v1": 19.7, "v2": 6, "v3": 145.0, "v4": 175},
        {"v1": 15.0, "v2": 8, "v3": 301.0, "v4": 335},
        {"v1": 21.4, "v2": 4, "v3": 121.0, "v4": 109},
    ]
    res3 = [
        {"v5": 3.90, "v6": 2.620, "v7": 16.46, "v8": 0, "v9": 1, "w0": 4, "w1": 4},
        {"v5": 3.90, "v6": 2.875, "v7": 17.02, "v8": 0, "v9": 1, "w0": 4, "w1": 4},
        {"v5": 3.85, "v6": 2.320, "v7": 18.61, "v8": 1, "v9": 1, "w0": 4, "w1": 1},
        {"v5": 3.08, "v6": 3.215, "v7": 19.44, "v8": 1, "v9": 0, "w0": 3, "w1": 1},
        {"v5": 3.15, "v6": 3.440, "v7": 17.02, "v8": 0, "v9": 0, "w0": 3, "w1": 2},
        {"v5": 2.76, "v6": 3.460, "v7": 20.22, "v8": 1, "v9": 0, "w0": 3, "w1": 1},
        {"v5": 3.21, "v6": 3.570, "v7": 15.84, "v8": 0, "v9": 0, "w0": 3, "w1": 4},
        {"v5": 3.69, "v6": 3.190, "v7": 20.00, "v8": 1, "v9": 0, "w0": 4, "w1": 2},
        {"v5": 3.92, "v6": 3.150, "v7": 22.90, "v8": 1, "v9": 0, "w0": 4, "w1": 2},
        {"v5": 3.92, "v6": 3.440, "v7": 18.30, "v8": 1, "v9": 0, "w0": 4, "w1": 4},
        {"v5": 3.92, "v6": 3.440, "v7": 18.90, "v8": 1, "v9": 0, "w0": 4, "w1": 4},
        {"v5": 3.07, "v6": 4.070, "v7": 17.40, "v8": 0, "v9": 0, "w0": 3, "w1": 3},
        {"v5": 3.07, "v6": 3.730, "v7": 17.60, "v8": 0, "v9": 0, "w0": 3, "w1": 3},
        {"v5": 3.07, "v6": 3.780, "v7": 18.00, "v8": 0, "v9": 0, "w0": 3, "w1": 3},
        {"v5": 2.93, "v6": 5.250, "v7": 17.98, "v8": 0, "v9": 0, "w0": 3, "w1": 4},
        {"v5": 3.00, "v6": 5.424, "v7": 17.82, "v8": 0, "v9": 0, "w0": 3, "w1": 4},
        {"v5": 3.23, "v6": 5.345, "v7": 17.42, "v8": 0, "v9": 0, "w0": 3, "w1": 4},
        {"v5": 4.08, "v6": 2.200, "v7": 19.47, "v8": 1, "v9": 1, "w0": 4, "w1": 1},
        {"v5": 4.93, "v6": 1.615, "v7": 18.52, "v8": 1, "v9": 1, "w0": 4, "w1": 2},
        {"v5": 4.22, "v6": 1.835, "v7": 19.90, "v8": 1, "v9": 1, "w0": 4, "w1": 1},
        {"v5": 3.70, "v6": 2.465, "v7": 20.01, "v8": 1, "v9": 0, "w0": 3, "w1": 1},
        {"v5": 2.76, "v6": 3.520, "v7": 16.87, "v8": 0, "v9": 0, "w0": 3, "w1": 2},
        {"v5": 3.15, "v6": 3.435, "v7": 17.30, "v8": 0, "v9": 0, "w0": 3, "w1": 2},
        {"v5": 3.73, "v6": 3.840, "v7": 15.41, "v8": 0, "v9": 0, "w0": 3, "w1": 4},
        {"v5": 3.08, "v6": 3.845, "v7": 17.05, "v8": 0, "v9": 0, "w0": 3, "w1": 2},
        {"v5": 4.08, "v6": 1.935, "v7": 18.90, "v8": 1, "v9": 1, "w0": 4, "w1": 1},
        {"v5": 4.43, "v6": 2.140, "v7": 16.70, "v8": 0, "v9": 1, "w0": 5, "w1": 2},
        {"v5": 3.77, "v6": 1.513, "v7": 16.90, "v8": 1, "v9": 1, "w0": 5, "w1": 2},
        {"v5": 4.22, "v6": 3.170, "v7": 14.50, "v8": 0, "v9": 1, "w0": 5, "w1": 4},
        {"v5": 3.62, "v6": 2.770, "v7": 15.50, "v8": 0, "v9": 1, "w0": 5, "w1": 6},
        {"v5": 3.54, "v6": 3.570, "v7": 14.60, "v8": 0, "v9": 1, "w0": 5, "w1": 8},
        {"v5": 4.11, "v6": 2.780, "v7": 18.60, "v8": 1, "v9": 1, "w0": 4, "w1": 2},
    ]

    res = pd.DataFrame([dict(**i, **j, **k) for i, j, k in zip(res1, res2, res3)])
    res.columns = [
        "name",
        "mpg",
        "cyl",
        "disp",
        "hp",
        "drat",
        "wt",
        "qsec",
        "vs",
        "am",
        "gear",
        "carb",
    ]
    return res


# qry methods


def head(self):
    """Fetch top 5 rows of the sub query table.

    Returns
    -------
    pandas.DataFrame

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>>
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").where("col1 < 5").head()
            col1  col2
        0     0    10
        1     1    11
        2     2    12
        3     3    13
        4     4    14
    """
    return self.run("select * from x limit 5")


def top(self):
    """Fetch top row of the sub query table.

    Returns
    -------
    pandas.Series

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>>
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").where("col1 < 5").head()
        col1     0
        col2    10
        Name: 0, dtype: int64
    """
    res = self.run("select * from x limit 1")
    if res.empty:
        return pd.Series(index=res.columns)
    else:
        return res.iloc[
            0,
        ]


def cols(self):
    """Fetch column names of the sub query table.

    Returns
    -------
    Column names as list of str

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>>
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").cols()
        ['col1', 'col2']
    """
    return self.run("select * from x where 1=2").columns.tolist()


def len(self):
    """Length of the sub query table.

    Returns
    -------
    int

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>>
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").len()
        10
    """
    return self.run("select count(1) from x").iloc[0, 0]


def dist(self, *args):
    """Count number of distinct occurances of data.

    Works on specified columns, or combination of columns, of the sub query table.

    Parameters
    ----------
    *args : str or [str]
        Either column names as str, or iterator of column name str.

    Returns
    -------
    pandas.Series

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>>
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").where("col1 < 5").dist('col1','col2',['col1','col2'])
        count(distinct col1)                   5
        count(distinct col2)                   5
        count(distinct col1 || '_' || col2)    5
        Name: 0, dtype: int64
    """
    _ = (" || '_' || ".join(_) if not isinstance(_, str) else _ for _ in args)
    _ = "".join(
        f"    ,count(distinct {j})\n" if i else f"    count(distinct {j})\n"
        for i, j in enumerate(_)
    )
    _ = "select \n" f"{_}" "from x"
    return self.run(_).iloc[0, :]


def mimx(self, col):
    """Fetch maximum and minimum values of a column.

    Parameters
    ----------
    col : str
        Column name as str.

    Returns
    -------
    pandas.Series

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>>
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").where("col1 < 5").mimx('col1')
        max(col1)    4
        min(col1)    0
        Name: 0, dtype: int64
    """
    _ = "select \n" f"    max({col}),min({col})\n" "from x"
    return self.run(_).iloc[0, :]


def valc(self, group_by, agg=None, order_by=None, n=True):
    """Value count of a column or combination of columns.

    A value count is a
    group by query, with total number of row of each group calculated.
    Also allow custom summary calculation, and custom order by clauses
    to be added.

    Parameters
    ----------
    group_by : str
        Group by clause as str.
    agg : str
        Custom aggeregation clause as str.
    order_by : str
        Order by clause as str.
    n : Bool
        Should the value count column be automatically created or not. Default
        to be True.

    Returns
    -------
    pandas.DataFrame

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>>
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").case("cat","col1 > 3 then 'A'",els = "'B'") \\
    ... .where("col1 < 5").valc('cat',"sum(col2) col2")
           cat  n  col2
        0   B   4    46
        1   A   1    14
    """
    group_by_cls = ",".join(group_by) if not isinstance(group_by, str) else group_by
    if agg is None:
        agg_cls = ""
    elif isinstance(agg, str):
        agg_cls = f"    ,{agg}\n"
    else:
        agg_cls = "".join(f"    ,{_}\n" for _ in agg)
    if order_by is None:
        if n:
            order_by_cls = "n desc"
        else:
            order_by_cls = group_by_cls
    else:
        order_by_cls = order_by
    _ = (
        "select \n"
        f"    {group_by_cls}\n"
        f"{f'    ,count(1) n{chr(10)}' if n else ''}"
        f"{agg_cls}"
        "from x\n"
        f"group by {group_by_cls}\n"
        f"order by {order_by_cls}"
    )
    return self.run(_)


def hash(self, *args):
    """Calculate a simple oracle hash for table.

    Arrive at a indicative hash value for a number of columns or all columns of
    a sub query table.
    Hash value is a number or symbol that is calculated from data
    , and is sensitive to any small changes in data. It serves as method to
    detect if any data element in data is changed.

    Parameters
    ----------
    *args : str
        Column names in str. If no value is given, a cols method will be
        performed to fetch the list of all columns, from which a hash will be
        calculated.

    Returns
    -------
    int

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import oc
    >>>
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> oc.drop('test')
    >>> oc.create('test',{'col1':'int','col2':'int'})
    >>> oc.write(tbl,'test')
    >>> oc.qry("test").where("col1 < 5").hash()
    """
    raise NotImplementedError


def piv(self):
    pass
