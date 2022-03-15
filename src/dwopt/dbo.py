import sqlalchemy as alc
import pandas as pd
import numpy as np
import logging
import re
from dwopt._qry import PgQry, LtQry, OcQry
from dwopt.set_up import _make_iris_df, _make_mtcars_df

_logger = logging.getLogger(__name__)


def db(eng):
    """Database operator object factory.

    Args
    -----------
    eng: str, or sqlalchemy.engine.Engine
        A `sqlalchemy engine url <https://docs.sqlalchemy.org/en/14/
        core/engines.html#database-urls>`_, which
        combines the user name, password, database names, etc.

        Alternatively a Database connection engine to be used.
        Use the :func:`dwopt.make_eng` function to make engine.

    Returns
    -------
    dwopt.dbo.Pg, or dwopt.dbo.Lt, or dwopt.dbo.Oc
        The relevant database operator object.

    Examples
    -------------
    Sqlite:

    >>> from dwopt import db
    >>> d = db("sqlite://")
    >>> d.mtcars()
    >>> d.run('select count(1) from mtcars')
       count(1)
    0        32

    Postgre::

        from dwopt import db
        url = "postgresql://dwopt_tester:1234@localhost/dwopt_test"
        db(url).iris(q=True).len()
        150

    Use engine instead of url::

        from dwopt import db, make_eng
        eng = make_eng("sqlite://")
        db(eng).mtcars(q=1).len()

    Oracle::

        from dwopt import db, Oc
        url = "oracle://scott2:tiger@tnsname"
        isinstance(db(url), Oc)
    """
    if isinstance(eng, str):
        eng = alc.create_engine(eng)
    else:
        if not isinstance(eng, alc.engine.Engine):
            raise ValueError("Invalid eng, either engine url or engine")
    nme = eng.name
    if nme == "postgresql":
        return Pg(eng)
    elif nme == "sqlite":
        return Lt(eng)
    elif nme == "oracle":
        return Oc(eng)
    else:
        raise ValueError("Invalid engine, either postgre, sqlite, or oracle")


def Db(eng):
    """Alias for :func:`dwopt.db`"""
    return db(eng)


class _Db:
    """
    The base database operator class.

    See examples for quick-start.

    Instantiate the child classes for different databases via one of below ways:

    * The factory function: :func:`dwopt.db`.
    * The pre-instantiated objects on package import.
    * The relevant child classes.

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


    Args
    ----------
    eng: str, or sqlalchemy.engine.Engine
        A `sqlalchemy engine url <https://docs.sqlalchemy.org/en/14/
        core/engines.html#database-urls>`_, which
        combines the user name, password, database names, etc.

        Alternatively a Database connection engine to be used.
        Use the :func:`dwopt.make_eng` function to make engine.

    Attributes
    ----------
    eng: sqlalchemy.engine.Engine
        Underlying engine. Details see
        `sqlalchemy.engine.Engine <https://docs.sqlalchemy.org/en/14/core/
        connections.html#sqlalchemy.engine.Engine>`_
    meta: sqlalchemy.schema.MetaData
        Underlying metadata. Details see
        `sqlalchemy.schema.MetaData <https://docs.sqlalchemy.org/en/14/core/
        metadata.html#sqlalchemy.schema.MetaData>`_

    Examples
    --------
    Instantiate and use a Sqlite database operator object via factory:

    >>> from dwopt import db
    >>> d = db("sqlite://")
    >>> d.mtcars()
    >>> d.run('select count(1) from mtcars')
       count(1)
    0        32

    Use the pre-instantiated Sqlite database operator object:

    >>> from dwopt import lt
    >>> lt.iris()
    >>> lt.run('select count(1) from iris')
       count(1)
    0       150

    Instantiate and use a Postgre database operator object via the class:

    >>> from dwopt import Pg
    >>> p = Pg("postgresql://dwopt_tester:1234@localhost/dwopt_test")
    >>> p.mtcars(q=1).len()
    32
    """

    def __init__(self, eng):
        if isinstance(eng, str):
            self.eng = alc.create_engine(eng)
        else:
            self.eng = eng
        self.meta = alc.MetaData()

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

    def _guess_dtype(self, dtype):
        """See :meth:`dwopt.dbo._Db.create`"""
        if np.issubdtype(dtype, np.int64):
            return alc.Integer
        elif np.issubdtype(dtype, np.float64):
            return alc.Float
        elif np.issubdtype(dtype, np.datetime64):
            return alc.DateTime
        else:
            return alc.String

    def _parse_sch_tbl_nme(self, sch_tbl_nme, split=True):
        """Resolve schema dot table name name into lower case components.

        Args
        ------
        sch_tbl_nme: str
            Table name in form ``my_schema1.my_table1`` or ``my_table1``.
        split: bool
            Split form or not.

        Returns
        ----------
        str or (str, str, str)
            parsed names, all elements can be None.

        Examples
        ---------
        >>> import dwopt
        >>> d = dwopt.dbo._Db
        >>> f = lambda x:d._parse_sch_tbl_nme(d, x, split=True)
        >>> g = lambda x:d._parse_sch_tbl_nme(d, x)
        >>> for i in ['ab', 'Ab', 'ab.ab', 'Ab.Ab', 'Ab.Ab.Ab', '', None, 3]:
        ...     print(f"{i = }, {f(i) = }, {g(i) = }")
        i = 'ab', f(i) = ('ab', None, 'ab'), g(i) = 'ab'
        i = 'Ab', f(i) = ('ab', None, 'ab'), g(i) = 'ab'
        i = 'ab.ab', f(i) = ('ab.ab', 'ab', 'ab'), g(i) = 'ab.ab'
        i = 'Ab.Ab', f(i) = ('ab.ab', 'ab', 'ab'), g(i) = 'ab.ab'
        i = 'Ab.Ab.Ab', f(i) = ('ab.ab.ab', 'ab', 'ab.ab'), g(i) = 'ab.ab.ab'
        i = '', f(i) = ('', None, ''), g(i) = ''
        i = None, f(i) = (None, None, None), g(i) = None
        i = 3, f(i) = (None, None, None), g(i) = None
        """
        try:
            clean = sch_tbl_nme.lower()
            items = clean.split(".")
        except AttributeError:
            sch = None
            tbl_nme = None
            full_nme = None
        else:
            n = len(items)
            if n == 1:
                sch = None
                tbl_nme = items[0]
                full_nme = tbl_nme
            elif n == 2:
                sch = items[0]
                tbl_nme = items[1]
                full_nme = clean
            else:
                sch = items[0]
                tbl_nme = ".".join(items[1:n])
                full_nme = clean
        if split:
            return full_nme, sch, tbl_nme
        else:
            return full_nme

    def _remove_sch_tbl(self, sch_tbl_nme):
        """Remove sch_tbl from meta.

        Args
        ------
        sch_tbl_nme: str
            Table name in form ``my_schema1.my_table1`` or ``my_table1``.

        Examples
        -----------
        Set-up::

            from dwopt import pg
            import sqlalchemy as alc
            meta = pg.meta

        First table entry into meta overwrites second one::

            meta.clear()
            alc.Table('test', meta, schema='test')
            alc.Table('test.test', meta)
            meta.tables
            meta.clear()
            alc.Table('test.test', meta)
            alc.Table('test', meta, schema='test')
            meta.tables

        No schema is entered unless explicitly::

            meta.clear()
            alc.Table('test.test', meta, schema=None)
            meta.clear()
            alc.Table('test.test.test', meta)
            meta.clear()
            alc.Table('test.test', meta, schema='test')

        Items removed by key not certain on schema::

            meta.clear()
            alc.Table('test', meta)
            alc.Table('test.test', meta)
            alc.Table('test.test.test', meta)
            meta.tables['test']
            meta.tables['test.test']
            meta.tables['test.test.test']
            meta.tables
        """
        if sch_tbl_nme in self.meta.tables:
            self.meta.remove(self.meta.tables[sch_tbl_nme])

    def _run(self, sql, args=None):
        """Run sql statement with argument passing"""
        with self.eng.begin() as c:
            _logger.info(f"running:\n{sql}")
            if args is not None:
                _logger.info(f"{len(args) = }")
                r = c.execute(alc.text(sql), args)
            else:
                r = c.execute(sql)
            _logger.info("done")
            if r.returns_rows:
                return pd.DataFrame(r.all(), columns=r.keys())

    def add_pkey(self, sch_tbl_nme, pkey):
        """Make and run an add primary key statement.

        Work on postgre and oracle.

        Args
        ----------
        sch_tbl_nme: str
            Table name in form ``my_schema1.my_table1`` or ``my_table1``.
        pkey : str
            columns names in form "col1, col2, ...".

        Examples
        --------
        >>> from dwopt import pg
        >>> pg.mtcars()
        >>> pg.add_pkey('mtcars', 'name')
        """
        sql = f"alter table {sch_tbl_nme} add primary key ({pkey})"
        return self.run(sql)

    def create(self, sch_tbl_nme, dtypes=None, **kwargs):
        """
        Make and run a create table statment.

        Args
        ----------
        sch_tbl_nme: str
            Table name in form ``my_schema1.my_table1`` or ``my_table1``.
        dtypes : dict, optional
            Dictionary of column names to data types mappings.
        **kwargs :
            Convenient way to add mappings.
            Keyword to argument mappings will be added to the dtypes
            dictionary.
            The keyword cannot be one of the positional parameter names.

        Notes
        -----
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
        are supposed to be used to work with datetime data types stored as text.

        **Other statements**

        The ``dtypes`` mappings also allow other sql statements which are
        part of a create statement to be added
        (`sqlite other <https://sqlite.org/lang_createtable.html>`_,
        `postgre other <https://www.postgresql.org/docs/current/
        sql-createtable.html>`_,
        `oracle other <https://docs.oracle.com/en/database/oracle/
        oracle-database/21/sqlrf/CREATE-TABLE.html>`_).
        For example a primary key constraint.

        Examples
        --------
        >>> from dwopt import lt
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
        >>> lt.run("select * from test")
        Empty DataFrame
        Columns: [id, score, amt, cat, time]
        Index: []

        >>> lt.drop('test2')
        >>> lt.create('test2', id='integer', score='real', cat='text')
        >>> lt.run("select * from test2")
        Empty DataFrame
        Columns: [id, score, cat]
        Index: []
        """
        if dtypes is None:
            dtypes = kwargs
        else:
            dtypes.update(kwargs)
        cls = ""
        for col, dtype in dtypes.items():
            cls += f"\n    ,{col} {dtype}"
        self.run(f"create table {sch_tbl_nme}(" f"\n    {cls[6:]}" "\n)")

    def create_schema(self, sch_nme):
        """Make and run a create schema statement.

        Works on postgre.

        Args
        ----------
        sch_nme: str
            Schema name.

        Examples
        --------
        >>> from dwopt import pg
        >>> pg.create_schema('test')
        >>> pg.iris('test.iris').len()
        150
        """
        try:
            self.run(f"create schema {sch_nme}")
        except Exception as ex:
            if "already exists" in str(ex):
                pass
            else:
                raise (ex)

    def cwrite(self, df, sch_tbl_nme):
        """
        Create table and insert based on dataframe.

        Replace '.' by '_' in dataframe column names.

        Args
        ----------
        df : pandas.DataFrame
            Payload Dataframe with data to insert.
        sch_tbl_nme: str
            Table name in form ``my_schema1.my_table1`` or ``my_table1``.

        See also
        ----------
        Data types infered based on the :meth:`dwopt.dbo._Db.create` method notes.
        Datetime and reversibility issue see :meth:`dwopt.dbo._Db.write` method notes.

        Examples
        --------
        ::

            import pandas as pd
            from dw import lt
            tbl = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
            lt.drop('test')
            lt.cwrite(tbl, 'test')
            lt.run("select * from test")
        """
        df = df.copy()
        df.columns = [_.lower().replace(".", "_") for _ in df.columns]
        sch_tbl_nme, sch, tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
        self._remove_sch_tbl(sch_tbl_nme)
        tbl = alc.Table(
            tbl_nme,
            self.meta,
            *[alc.Column(col, self._guess_dtype(df[col].dtype)) for col in df.columns],
            schema=sch,
        )
        _logger.info("running:\ncreate statments")
        tbl.create(self.eng)
        _logger.info("done")
        self.write(df, sch_tbl_nme)

    def delete(self):
        """WIP"""
        raise NotImplementedError

    def drop(self, sch_tbl_nme):
        """Drop table if exist.

        Args
        ----------
        sch_tbl_nme : str
            Table name in form 'myschema.mytable', or 'mytable'.

        See also
        ----------
        :meth:`dwopt.dbo._Db.exist`

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.drop('iris')
        >>> lt.iris()
        >>> lt.drop('iris')
        >>> lt.list_tables()
        Empty DataFrame
        Columns: [type, name, tbl_name, rootpage, sql]
        Index: []

        >>> from dwopt import pg
        >>> pg.create_schema('test')
        >>> tbl = 'test.iris'
        >>> pg.iris(tbl)
        >>> pg.exist(tbl)
        True
        >>> pg.drop(tbl)
        >>> pg.exist(tbl)
        False
        """
        sch_tbl_nme, sch, tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
        self._remove_sch_tbl(sch_tbl_nme)
        with self.eng.connect() as conn:
            _logger.info("running:\ndrop statments")
            alc.Table(tbl_nme, self.meta, schema=sch).drop(conn, checkfirst=True)
            _logger.info("done")

    def exist(self, sch_tbl_nme):
        """Check if table exist.

        Args
        ------
        sch_tbl_nme: str
            Table name in form ``my_schema1.my_table1`` or ``my_table1``.

        Returns
        ----------
        bool

        Examples
        ---------
        >>> from dwopt import lt
        >>> lt.iris()
        >>> lt.drop('mtcars')
        >>> lt.exist('iris')
        True
        >>> lt.exist('mtcars')
        False

        >>> from dwopt import pg as d
        >>> d.create_schema('test')
        >>> d.iris('test.iris')
        >>> d.drop('test.mtcars')
        >>> d.exist('test.iris')
        True
        >>> d.exist('test.mtcars')
        False
        """
        sch_tbl_nme, sch, tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
        self._remove_sch_tbl(sch_tbl_nme)
        try:
            self.meta.reflect(self.eng, schema=sch, only=[tbl_nme])
            return True
        except Exception as ex:
            if "Could not reflect: requested table(s) not available in Engine" in str(
                ex
            ):
                _logger.debug(ex)
                return False
            else:
                raise ex

    def iris(self, sch_tbl_nme="iris", q=False):
        """Create the iris test table on the database.

        Drop and recreate if already exist.
        Sourced from `UCI iris <https://archive.ics.uci.edu/ml/datasets/Iris/>`_.

        args
        -------
        sch_tbl_nme: str
            Table name in form ``my_schema1.my_table1`` or ``my_table1``.
            Default "iris".
        q: bool
            Return query object or not. Default False.

        Returns
        -------
        None or dwopt._qry._Qry
            Query object with sch_tbl_nme loaded for convenience.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.iris()
        >>> lt.run('select count(*) from iris')
           count(*)
        0       150

        >>> from dwopt import lt
        >>> lt.iris(q=True).valc('species', 'avg(petal_length)')
           species   n  avg(petal_length)
        0  sicolor  50              4.260
        1   setosa  50              1.462
        2  rginica  50              5.552

        >>> from dwopt import pg
        >>> pg.create_schema('test')
        >>> pg.iris('test.iris', q=1).len()
        150
        """
        sch_tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme, split=False)
        self.drop(sch_tbl_nme)
        self.cwrite(_make_iris_df(), sch_tbl_nme)
        if q:
            return self.qry(sch_tbl_nme)

    def list_cons(self):
        """
        List all constraints.

        Only works for postgre.

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

    def mtcars(self, sch_tbl_nme="mtcars", q=False):
        """Create the mtcars test table on the database.

        Drop and recreate if already exist.
        Sourced from `R mtcars <https://www.rdocumentation.org/packages/datasets
        /versions/3.6.2/topics/mtcars>`_.

        args
        -------
        sch_tbl_nme: str
            Table name in form ``my_schema1.my_table1`` or ``my_table1``.
            Default "mtcars".
        q: bool
            Return query object or not. Default False.

        Returns
        -------
        None or dwopt._qry._Qry
            Query object with sch_tbl_nme loaded for convenience.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.mtcars()
        >>> lt.run('select count(*) from mtcars')
           count(*)
        0        32

        >>> from dwopt import lt
        >>> lt.mtcars(q=True).valc('cyl', 'avg(mpg)')
           cyl   n   avg(mpg)
        0    8  14  15.100000
        1    4  11  26.663636
        2    6   7  19.742857

        >>> from dwopt import pg
        >>> pg.create_schema('test')
        >>> pg.mtcars('test.mtcars', q=1).len()
        32
        """
        sch_tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme, split=False)
        self.drop(sch_tbl_nme)
        self.cwrite(_make_mtcars_df(), sch_tbl_nme)
        if q:
            return self.qry(sch_tbl_nme)

    def qry(self, *args, **kwargs):
        """
        Make a query object.

        Different database operator object provide different query object,
        tailored to relevant databases.
        See the :doc:`query objects <qry>` section for details.

        Args
        ----------
        *args :
            Positional arguments of the :class:`dwopt._qry._Qry`>.
        **kwargs :
            keyword arguments of the :class:`dwopt._qry._Qry`.

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

    def run(self, sql=None, args=None, pth=None, mods=None, **kwargs):
        """
        Run sql statement.

        Support argument passing, text replacement
        and reading statements from sql script.

        Args
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
        Run sql::

            from dwopt import lt
            lt.iris()
            lt.run("select * from iris limit 1")

        Run sql with argument passing::

            lt.run("select count(1) from iris where species = :x",args = {'x':'setoso'})

        Run sql with text modification::

            old = 'iris'
            new = 'iris2'
            lt.run("create table :x as select * from :y",
                mods = {'x':new, 'y': old})
            lt.run("drop table :tbl", tbl = old)
            lt.run("select a.*, :col + 1 as :col_added from :tbl a"
                   , mods = {'tbl': new}, col = 'petal_length')
        """
        if sql is None and pth is not None:
            with open(pth) as f:
                sql = f.read()
            _logger.info(f"sql from:\n{pth}")
        if mods is not None or len(kwargs) > 0:
            sql = self._bind_mods(sql, mods, **kwargs)
        return self._run(sql, args)

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

    def update(self):
        """WIP"""
        raise NotImplementedError

    def write(self, df, sch_tbl_nme):
        """
        Make and run a insert many statement.

        This should follow from a :meth:`dwopt.dbo._Db.create` call which sets up
        the database table with table name, column names, intended data types,
        and constraints.

        Args
        ----------
        df: pandas.DataFrame
            Payload Dataframe with data to insert.
        sch_tbl_nme: str
            Table name in form ``my_schema1.my_table1`` or ``my_table1``.

        Notes
        -----

        **Datetime**

        Pandas Datetime64 columns are converted into object columns, and the
        ``pandas.NaT`` objects are converted into ``None`` before insertion.
        For sqlite tables, datetime columns should be manually converted to str
        and None before insertion.

        **Reversibility**

        Ideally python dataframe written to database should allow a exact same
        dataframe to be read back into python. Whether this is true depends on the
        database, data and object types on the dataframe,
        and data types on the database table.

        With the set up used in the :func:`dwopt.make_test_tbl` function, we have
        following results (Actual tests see the test script relevant for this
        method):

        * Postgre example has reversibility except for row ordering and auto generated
          pandas dataframe index. These can be strightened as below.

          .. code-block:: python

              df.reset_index(drop=True).sort_values('id')

        * Sqlite stores datetime datatypes as text, this causes a str type column to
          be read back. One strategy is to convert from datatime and NaT to
          str and None before insertion, and convert back to date and datetime
          when read back. Use ``datetime`` and ``pandas`` package for this.

          .. code-block:: python

              lt.write(
                  df.assign(
                      time=lambda x: x.time.astype(str).where(~x.time.isna(), None)),
                  "test2",
              )
              tbl = (
                  db.qry("test2").run()
                  .assign(
                      date=lambda x: x["date"].apply(
                          lambda x: datetime.date.fromisoformat(x) if x else None
                      ),
                      time=lambda x: pd.to_datetime(x.time),
                  )
              )

        * Oracle is similiar to postgre.

        Examples
        --------
        ::

            import pandas as pd
            from dw import lt
            tbl = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
            lt.drop('test')
            lt.create('test', col1='int', col2='text'})
            lt.write(tbl,'test')
            lt.run('select * from test')
        """
        L = len(df)
        sch_tbl_nme, sch, tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
        _logger.info(f"writing to {sch_tbl_nme}, len: {L}")
        if L == 0:
            return
        df = df.copy()
        cols = df.columns.tolist()
        for col in cols:
            if np.issubdtype(df[col].dtype, np.datetime64):
                df[col] = df[col].astype(object).where(~df[col].isna(), None)
        self._remove_sch_tbl(sch_tbl_nme)
        tbl = alc.Table(
            tbl_nme, self.meta, *[alc.Column(col) for col in cols], schema=sch
        )
        with self.eng.connect() as conn:
            _logger.info("running:\ninsert statments")
            conn.execute(
                tbl.insert(),
                df.to_dict("records"),
            )
            _logger.info("done")

    def write_nodup(self, tbl, sch_tbl_nme, pkey, where=None):
        """Insert without creating duplicates.

        Does below:

        1. Make and run a select statement with optionally provided
           where clause.
        2. If step 1 returns any results and the payload table in non-empty
           , remove duplicates on the payload table, using the provided primary
           key columns as judge of duplication.
        3. Make insert statement on the non-duplicating payload data via the
           :meth:`dwopt.dbo._Db.write` method.

        Args
        ----------
        tbl: pandas.DataFrame
            Payload Dataframe with data to insert.
        sch_tbl_nme: str
            Table name in form ``my_schema1.my_table1`` or ``my_table1``.
        pkey: [str]
            Iterable of column name str.
        where: str
            where clause in str form. The ``where`` keyword is not needed.

        See also
        --------
        :meth:`dwopt.dbo._Db.write`

        Examples
        --------
        >>> import pandas as pd
        >>> from dwopt import lt
        >>> tbl = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        >>> tbl2 = pd.DataFrame({'col1': [1, 3], 'col2': ['a', 'c']})
        >>> lt.drop('test')
        >>> lt.create('test', col1='int', col2='text')
        >>> lt.write(tbl, 'test')
        >>> lt.write_nodup(tbl2, 'test', ['col1'], "col1 < 4")
        >>> lt.run("select * from test")
           col1 col2
        0     1    a
        1     2    b
        2     3    c
        """
        cols = ",".join(pkey)
        where_cls = f"\nwhere {where}" if where else ""
        sch_tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme, split=False)
        db_tbl = self.run(f"select {cols} from {sch_tbl_nme} {where_cls}")
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
        _logger.debug(
            f"write nodup: {l_tbl = }, {l_db_tbl = }" f", {len(dedup_tbl) = }"
        )
        self.write(dedup_tbl, sch_tbl_nme)


class Pg(_Db):
    def qry(self, *args, **kwargs):
        return PgQry(self, *args, **kwargs)

    def _guess_dtype(self, dtype):
        if np.issubdtype(dtype, np.int64):
            return alc.dialects.postgresql.BIGINT
        elif np.issubdtype(dtype, np.float64):
            return alc.Float(8)
        else:
            return super(type(self), self)._guess_dtype(dtype)

    def list_cons(self):
        sql = "SELECT * FROM information_schema.constraint_table_usage"
        return self.run(sql)

    def list_tables(self):
        sql = (
            "select table_catalog,table_schema,table_name"
            "\n    ,is_insertable_into,commit_action"
            "\nfrom information_schema.tables"
            "\nwhere table_schema"
            "\n   not in ('information_schema','pg_catalog')"
        )
        return self.run(sql)

    def table_cols(self, sch_tbl_nme):
        sch_tbl_nme, sch, tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
        sql = (
            "select column_name, data_type from information_schema.columns"
            f"\nwhere table_schema = '{sch}' "
            f"\nand table_name = '{tbl_nme}'"
        )
        return self.run(sql)


class Lt(_Db):
    def qry(self, *args, **kwargs):
        return LtQry(self, *args, **kwargs)

    def _guess_dtype(self, dtype):
        if np.issubdtype(dtype, np.float64):
            return alc.REAL
        elif np.issubdtype(dtype, np.datetime64):
            return alc.String
        else:
            return super(type(self), self)._guess_dtype(dtype)

    def list_tables(self):
        sql = (
            "select * from sqlite_master "
            "\nwhere type ='table' "
            "\nand name NOT LIKE 'sqlite_%' "
        )
        return self.run(sql)


class Oc(_Db):
    def qry(self, *args, **kwargs):
        return OcQry(self, *args, **kwargs)

    def _guess_dtype(self, dtype):
        if np.issubdtype(dtype, np.int64):
            return alc.dialects.oracle.NUMBER
        else:
            return super(type(self), self)._guess_dtype(dtype)

    def list_tables(self, owner):
        sql = (
            "select/*+PARALLEL (4)*/ owner,table_name"
            "\n    ,max(column_name),min(column_name)"
            "\nfrom all_tab_columns"
            f"\nwhere owner = '{owner.upper()}'"
            "\ngroup by owner,table_name"
        )
        return self.run(sql)

    def table_cols(self, sch_tbl_nme):
        sch, tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
        sql = (
            "select/*+PARALLEL (4)*/ *"
            "\nfrom all_tab_columns"
            f"\nwhere owner = '{sch.upper()}'"
            f"\nand table_name = '{tbl_nme.upper()}'"
        )
        return self.run(sql)

    def table_sizes(self):
        sql = (
            "select/*+PARALLEL (4)*/"
            "\n    tablespace_name,segment_type,segment_name"
            "\n    ,sum(bytes)/1024/1024 table_size_mb"
            "\nfrom user_extents"
            "\ngroup by tablespace_name,segment_type,segment_name"
        )
        return self.run(sql)
