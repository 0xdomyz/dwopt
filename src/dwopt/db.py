from sqlalchemy.sql import text as _text
import pandas as pd
import numpy as np
import logging
import re
from dwopt._qry import PgQry, LtQry, OcQry

_logger = logging.getLogger(__name__)


class _Db:
    """
    The base database operator class.

    User should not instantiated the base class directly.
    Instead instantiate the child classes tailored for the intended database,
    or use the pre-instantiated objects on package import.
    See the notes and the examples.

    Parameters
    ----------
    eng : sqlalchemy engine
        Database connection engine to be used. Use the :func:`dwopt.make_eng` function
        to make engine.

    Notes
    -----
    The child classes and the pre-instantiated objects:

    ========== =================== ========================
    Database    Child class        Pre-instantiated object
    ========== =================== ========================
    Postgre     ``dwopt.Pg(eng)``   ``dwopt.pg``
    Sqlite      ``dwopt.Lt(eng)``   ``dwopt.lt``
    Oracle      ``dwopt.Oc(eng)``   ``dwopt.oc``
    ========== =================== ========================

    Pre-instantiation uses the default credentials set-up prior by the user
    via the :func:`dwopt.save_url` function.

    Examples
    --------
    Instantiate and use a Postgre database operator object.

    >>> from dwopt import make_eng, Pg
    >>> url = "postgresql://dwopt_tester:1234@localhost/dwopt_test"
    >>> pg = Pg(make_eng(url))
    >>> pg.run('select count(1) from test')
        count(1)
    0         42

    Use the pre-instantiated Sqlite database operator object.

    >>> from dwopt import lt
    >>> pg.run('select count(1) from test')
        count(1)
    0         42

    Note: use the :func:`dwopt.make_test_tbl` function to make testing data for above.

    >>> from dwopt import lt, make_test_tbl
    >>> _ = make_test_tbl(lt, 'test', 42)
    """

    def __init__(self, eng):
        self.eng = eng
        self.meta = {}

    def update_meta(self, meta):
        self.meta.update({meta.schema, meta})

    def _parse_sch_tbl_nme(self, sch_tbl_nme):
        """Resolve schema dot table name name into components"""
        _ = sch_tbl_nme.split(".")
        n = len(_)
        if n == 1:
            sch = None
            tbl_nme = _[0]
        elif n == 2:
            sch = _[0]
            tbl_nme = _[1]
        else:
            sch = _[0]
            tbl_nme = ".".join(_[1:])
            tbl_nme = f'"{tbl_nme}"'
        return sch, tbl_nme

    def run(self, sql=None, args=None, pth=None, mods=None, **kwargs):
        """
        Run sql statement.

        Support argument passing, text replacement
        and reading statements from sql script.

        Parameters
        ----------
        sql : str, optional
            The sql statement to run. Only 1 statement is allowed.
        args : dict, or [dict], optional
            Dictionary or list of dictionary of argument name str to argument
            data object mappings.
            These argument data objects are passed via sqlalchemy to the database,
            to function as data for the argument names.
            See the notes and the examples section for details.
        pth : str, optional
            Path to sql script, ignored if the sql parameter is not None.
            The script can hold one and only one sql statement, typically
            a significant piece of table creation statement.
        mods : dict, optional
            Dictionary of modification name str to modification str mappings.
            Replaces modification name in the sql by the respective
            modification str.
            See the notes and the examples section for details.
        **kwargs :
            Convenient way to add modification mappings.
            Keyword to argument mappings will be added to the mods dictionary.
            The keyword cannot be one of the positional parameter names.

        Returns
        -------
        pandas.DataFrame or None
            Returns dataframe if the database returns any result.
            Returns dataframe with column names and zero rows if running query
            that returns zero rows.
            Returns None otherwise, typically when running DDL/DML statement.

        Notes
        -----

        **The args and the mods parameter**

        An argument name is denoted in the sql by prepending
        a colon symbol ``:`` before a str.

        Similiarly, a modification name is denoted by prepending a
        colon symbol ``:`` before a str in the sql.
        The end of str is to be followed by a symbol other than
        a lower or upper case letter, or a number.
        It is also ended before a line break.

        The args parameter method of passing arguments is less prone
        to unintended sql injection, while the mods paramter method of
        text replacement gives much more flexibility when it comes to
        programatically generate sql statment.

        Examples
        --------
        Make example table on sqlite.

        >>> import pandas as pd
        >>> from dw import lt
        >>> tbl = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        >>> lt.drop('test')
        >>> lt.create('test',{'col1':'int','col2':'int'})
        >>> lt.write(tbl,'test')

        Run sql.

        >>> lt.run("select * from test")
            col1  col2
        0     1     3
        1     2     4

        Run sql with argument passing.

        >>> lt.run("select * from test where col1 = :cond",args = {'cond':2})
            col1  col2
        0     2     4

        Run sql with text modification.

        >>> tbl_nme = 'test2'
        >>> lt.run("drop table :tbl",mods = {'tbl':tbl_nme})
        >>> lt.run("create table :tbl as select * from test where :col = 1"
        >>>        , mods = {'tbl':tbl_nme}, col = 'col1')
        >>> lt.run("select *,col2 + 1 as :col from :tbl_nme"
        >>>        , tbl_nme = tbl_nme, col = 'col3')
            col1  col2  col3
        0     1     3     4
        """
        if sql is None and pth is not None:
            with open(pth) as f:
                sql = f.read()
            _logger.info(f"sql from:\n{pth}")
        if mods is not None or len(kwargs) > 0:
            sql = self._bind_mods(sql, mods, **kwargs)
        return self._run(sql, args)

    def _run(self, sql, args=None):
        """Run sql statement with argument passing"""
        with self.eng.begin() as c:
            _logger.info(f"running:\n{sql}")
            if args is not None:
                _logger.info(f"{len(args) = }")
                r = c.execute(_text(sql), args)
            else:
                r = c.execute(sql)
            _logger.info("done")
            if r.returns_rows:
                return pd.DataFrame(r.all(), columns=r.keys())

    def _bind_mods(self, sql, mods=None, **kwargs):
        """Apply modification to sql statement"""
        if mods is None:
            mods = kwargs
        else:
            mods.update(kwargs)
        for i, j in mods.items():
            sql = re.sub(f":{i}(?=[^a-zA-Z0-9]|$)", str(j), sql)
            _logger.debug(f"replaced :{i} by {j}")
        return sql

    def create(self, tbl_nme, dtypes=None, **kwargs):
        """
        Make and run a create table statment.

        Parameters
        ----------
        tbl_nme : str
            Name of the table to create.
        dtypes : dict, optional
            Dictionary of column names to data types mappings.
        **kwargs :
            Convenient way to add mappings.
            Keyword to argument mappings will be added to the dtypes
            dictionary.
            The keyword cannot be one of the positional parameter names.

        Notes
        -----

        **Statement used**

        .. code-block:: sql

            create table tbl_nme(
                ,col1 dtype1
                ,col2 dtype2
                ...
            )

        **Datatypes**

        Datatypes vary across databses
        (`postgre type <https://www.postgresql.org/docs/current/
        datatype.html>`_,
        `sqlite type <https://www.sqlite.org/datatype3.html>`_,
        `oracle type <https://docs.oracle.com/en/database/oracle/
        oracle-database/21/sqlqr/Data-Types.html>`_),
        common example below:

        ========== =========== ======= ============
        Type       Postgre     Sqlite  Oracle
        ========== =========== ======= ============
        integer    bigint      integer number
        float      float8      real    float
        string     varchar(20) text    varchar2(20)
        datetime   timestamp   text    timestamp
        date       date        text    date
        ========== =========== ======= ============

        Note `sqlite datetime functions <https://www.sqlite.org/lang_datefunc.html>`_
        could be used to work with date data types stored as text.

        **Other statements**

        The dtypes mappings also allow other sql statements which are
        part of a create statement to be added
        (`sqlite other <https://sqlite.org/lang_createtable.html>`_,
        `postgre other <https://www.postgresql.org/docs/current/
        sql-createtable.html>`_,
        `oracle other <https://docs.oracle.com/en/database/oracle/
        oracle-database/21/sqlrf/CREATE-TABLE.html>`_).
        For example a primary key constraint.

        Examples
        --------

        Make example table on sqlite.

        >>> from dw import lt
        ...
        >>> lt.drop('test')
        >>> lt.create('test'
        ...     ,{
        ...         'id': 'integer'
        ...         ,'score': 'real'
        ...         ,'amt': 'integer'
        ...         ,'cat': 'text'
        ...         ,'time': 'text'
        ...         ,'constraint df_pk': 'primary key (id)'
        ...     })
        """
        if dtypes is None:
            dtypes = kwargs
        else:
            dtypes.update(kwargs)
        cls = ""
        for col, dtype in dtypes.items():
            cls += f"\n    ,{col} {dtype}"
        self.run(f"create table {tbl_nme}(" f"\n    {cls[6:]}" "\n)")

    def write(self, df, tbl_nme):
        """
        Make and run a insert many statement.

        This should follow from a :meth:`dwopt.db._Db.create` call which sets up
        the database table with column names, intended data types, and constraints.

        Parameters
        ----------
        df : pandas.DataFrame
            Payload Dataframe with data to insert.
        tbl_nme : str
            Name of the database table to insert into.

        Notes
        -----

        **Statements used**

        .. code-block:: sql

            insert into tbl_nme (col1, col2, ...)
            values (:col1, :col2, ...)

        With arguments to statement being the Dataframe in dictionary form:

        .. code-block:: python

            [
                {'col1' : data1, 'col2' : data2, ...}
                ,{'col1' : data3, 'col2' : data4, ...}
                ...
            ]

        **Datetime**

        Pandas Datetime64 columns are converted into object columns, and the
        ``pandas.NaT`` objects are converted into ``None`` before insertion.
        Consider convert datetime columns to str columns before insertion on sqlite
        tables.

        **Reversibility**

        Below examples use the :func:`dwopt.make_test_tbl` function to make testing
        python dataframes with various data types,
        and insert them into databases with relevant data types.
        Then attempt to read the table back into python to reconcile with the original.

        Sqlite date and datetime columns are read as str columns.
        Use ``datetime`` and ``pandas`` package to convert back to
        python date and datetime columns.

        .. code-block:: python

            from dwopt import make_test_tbl
            import pandas as pd
            import datetime
            from pandas.testing import assert_frame_equal

            lt, df = make_test_tbl('lt', 'test')
            tbl = lt.qry('test').run().assign(
                date = lambda x:x["date"].apply(
                    lambda x:datetime.date.fromisoformat(x) if x else None
                ),
                time = lambda x:pd.to_datetime(x.time)
            )
            assert_frame_equal(tbl, df)

        Examples
        --------

        Make example table on sqlite, then insert rows into it.

        >>> import pandas as pd
        >>> from dw import lt
        >>>
        >>> tbl = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        >>> lt.drop('test')
        >>> lt.create('test',{'col1':'int','col2':'text'})
        >>> lt.write(tbl,'test')
        """
        L = len(df)
        _logger.debug(f"writing to {tbl_nme}, len: {L}")
        if L == 0:
            return
        df = df.copy()
        cols = df.columns.tolist()
        for col in cols:
            if np.issubdtype(df[col].dtype, np.datetime64):
                df[col] = df[col].astype(object).where(~df[col].isna(), None)
        _ = ",".join(f":{i}" for i in cols)
        sql = f"insert into {tbl_nme} ({','.join(cols)})" f" values ({_})"
        self.run(sql, args=df.to_dict("records"))

    def write_nodup(self, tbl, tbl_nme, pkey, where=None):
        """Insert many statement without creating duplicates.

        Does below:

        1. Make and run a select statement with optionally provided
           where clause.
        2. If step 1 returns any results and the payload table in non-empty
           , remove duplicates on the payload table, using the provided primary
           key columns as judge of duplication.
        3. Make insert statement on the non-duplicating payload data.

        Parameters
        ----------
        tbl : pandas.DataFrame
            Payload Dataframe with data to insert.
        tbl_nme : str
            Name of the database table to insert into.
        pkey : [str]
            Iterable of column name str.
        where : str
            where clause in str form. The ``where`` keyword is not needed.

        See also
        --------
        :meth:dwopt.db._Db.write

        Examples
        --------

        Make example table on sqlite, then insert duplicated rows into it.

        >>> import pandas as pd
        >>> from dw import lt
        >>>
        >>> tbl = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        >>> tbl2 = pd.DataFrame({'col1': [1, 3], 'col2': ['a', 'c']})
        >>> lt.drop('test')
        >>> lt.create('test',{'col1':'int','col2':'text'})
        >>> lt.write(tbl,'test')
        >>> lt.write_nodup(tbl2,'test',['col1'],"col1 < 4")
        >>> lt.run("select * from test")
            col1  col2
        0     1     a
        1     2     b
        2     3     c
        """
        cols = ",".join(pkey)
        where_cls = f"\nwhere {where}" if where else ""
        db_tbl = self.run(f"select {cols} from {tbl_nme} {where_cls}")
        l_tbl = len(tbl)
        l_db_tbl = len(db_tbl)
        if l_tbl > 0 and l_db_tbl > 0:
            dedup_tbl = (
                tbl.merge(
                    db_tbl, how="left", on=pkey, validate="one_to_one", indicator=True
                )
                .loc[lambda x: x._merge == "left_only", :]
                .drop(columns="_merge")
            )
        else:
            dedup_tbl = tbl
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"write nodup: {l_tbl = }, {l_db_tbl = }" f", {len(dedup_tbl) = }"
            )
        self.write(dedup_tbl, tbl_nme)

    def drop(self, tbl_nme):
        """
        Make and run a drop table statement. Does not throw error if table
        not exist. Example sql code:

        .. code-block:: sql

            drop tbl_nme

        Oracle sql code:

        .. code-block:: sql

            drop tbl_nme purge

        Parameters
        ----------
        tbl_nme : str
            Name of the database table to drop.

        Returns
        -------
        str
            ``tbl_nme not exist`` if table not exist
            , ``tbl_nme dropped`` otherwise.

        Notes
        -----

        *Error catching*

        Does not catch specifically the error related to table not exist in
        this version.

        Examples
        --------

        Make example table on sqlite, then drop it.

        >>> from dw import lt
        >>>
        >>> lt.drop('test')
        >>> lt.create('test',{'col1':'int','col2':'text'})
        >>> lt.drop('test')
        """
        try:
            self.run(f"drop table {tbl_nme}")
        except Exception as ex:
            _logger.debug(str(ex))
            return f"{tbl_nme} not exist"
        else:
            return f"{tbl_nme} dropped"

    def update(self):
        pass

    def delete(self):
        pass

    def add_pkey(self, tbl_nme, pkey):
        """
        Make and run an add primary key statement. Does not work on sqlite.
        Example sql code:

        .. code-block:: sql

            alter table tbl_nme add primary key (col1, col2, ...)

        Parameters
        ----------
        tbl_nme : str
            Name of the database table to operate on.
        tbl_nme : str
            columns names in form ``col1, col2, ...``.

        Examples
        --------

        Make example table on postgre, then add primary key constraint.

        >>> from dw import pg
        >>>
        >>> pg.drop('test')
        >>> pg.create('test',{'col1':'int','col2':'text'})
        >>> pg.add_pkey('test', 'col1, col2')
        """
        sql = f"alter table {tbl_nme} add primary key ({pkey})"
        return self.run(sql)

    def qry(self, *args, **kwargs):
        """
        Make a query object.

        Different database operator object provide different query object,
        tailored to relevant databases.
        See the :doc:`query objects <qry>` section for details.

        Parameters
        ----------
        *args :
            Positional arguments of the :class:`query class <dwopt._qry._Qry>`.
        **kwargs :
            keyword arguments of the :class:`query class <dwopt._qry._Qry>`.

        Returns
        -------
        dwopt._qry._Qry

        Examples
        --------

        Make query object from sqlite database operator object.

        >>> from dwopt import lt
        >>> lt.qry("test").where("x>5").print()
            select * from test
            where x>5
        """
        raise NotImplementedError

    from dwopt._sqls.base import list_tables
    from dwopt._sqls.base import table_sizes
    from dwopt._sqls.base import table_cols
    from dwopt._sqls.base import list_cons


class Pg(_Db):
    def qry(self, *args, **kwargs):
        return PgQry(self, *args, **kwargs)

    from dwopt._sqls.pg import list_tables
    from dwopt._sqls.pg import table_cols
    from dwopt._sqls.pg import list_cons


class Lt(_Db):
    def qry(self, *args, **kwargs):
        return LtQry(self, *args, **kwargs)

    from dwopt._sqls.lt import list_tables


class Oc(_Db):
    def drop(self, tbl_nme):
        try:
            self.run(f"drop table {tbl_nme} purge")
        except Exception as ex:
            _logger.debug(str(ex))
            return f"{tbl_nme} not exist"
        else:
            return f"{tbl_nme} dropped"

    def qry(self, *args, **kwargs):
        return OcQry(self, *args, **kwargs)

    from dwopt._sqls.oc import list_tables
    from dwopt._sqls.oc import table_sizes
    from dwopt._sqls.oc import table_cols
