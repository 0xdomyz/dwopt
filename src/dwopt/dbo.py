import sqlalchemy as alc
import pandas as pd
import numpy as np
import datetime
import logging
import re
from dwopt._qry import _Qry
from dwopt.set_up import _make_iris_df, _make_mtcars_df

_logger = logging.getLogger(__name__)


def db(eng):
    """The :class:`database operator object <dwopt.dbo._Db>` factory.

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
    dwopt.dbo._Db
        The relevant database operator object.

    Examples
    -------------
    Produce a sqlite database operator object:

    >>> from dwopt import db
    >>> d = db("sqlite://")
    >>> d.mtcars()
    >>> d.run('select count(1) from mtcars')
       count(1)
    0        32

    Produce a postgre database operator object:

    >>> from dwopt import db
    >>> url = "postgresql://dwopt_tester:1234@localhost/dwopt_test"
    >>> db(url).iris(q=True).len()
    150

    Produce using engine object:

    >>> from dwopt import db, make_eng
    >>> eng = make_eng("sqlite://")
    >>> db(eng).mtcars(q=1).len()
    32

    Produce an oracle database operator object:

    >>> from dwopt import db, Oc
    >>> url = "oracle://scott2:tiger@tnsname"
    >>> isinstance(db(url), Oc)
    True
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
    >>> lt.qry('iris').len()
    150

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
        _nme = self.eng.name
        if _nme == "postgresql":
            self._dialect = "pg"
        elif _nme == "sqlite":
            self._dialect = "lt"
        elif _nme == "oracle":
            self._dialect = "oc"

    def _bind_mods(self, sql, mods=None, **kwargs):
        """Apply modification to sql statement

        Examples
        -----------
        import re
        def f(sql, i, j):
            return re.sub(f":{i}(?=[^a-zA-Z0-9]|$)", str(j), sql)
        f("from tbl_:yr_0304", 'yr', 2017)
        f(f("from tbl_:yr_:yr1_0304", 'yr', 2017), 'yr1', 2018)
        f("from tbl_:yr_mth_tbl", 'yr_mth', 2017)
        """
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
        if self._dialect == "pg":
            if np.issubdtype(dtype, np.int64):
                return alc.dialects.postgresql.BIGINT
            elif np.issubdtype(dtype, np.float64):
                return alc.Float(8)
        elif self._dialect == "lt":
            if np.issubdtype(dtype, np.float64):
                return alc.REAL
            elif np.issubdtype(dtype, np.datetime64):
                return alc.String
        elif self._dialect == "oc":
            if np.issubdtype(dtype, np.int64):
                return alc.dialects.oracle.NUMBER
            elif np.issubdtype(dtype, np.float64):
                return alc.Float
            elif np.issubdtype(dtype, np.datetime64):
                return alc.DateTime
            else:
                return alc.String(20)
        else:
            raise ValueError("invalid dialect, only 'pg', 'lt', or 'oc'")

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
        >>> pg.qry('information_schema.constraint_table_usage').select(
        ...     'table_name, constraint_name').where(
        ...     "table_schema = 'public'", "table_name = 'mtcars'").run()
          table_name constraint_name
        0     mtcars     mtcars_pkey
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
        dtypes : {str:str}, optional
            Dictionary of column names to data types mappings.
        **kwargs :
            Convenient way to add mappings.
            Keyword to argument mappings will be added to the dtypes
            dictionary.

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
        >>> lt.create(
        ...     'test',
        ...     {
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

        * Replace ``.`` by ``_`` in dataframe column names.
        * Data types infered based on the :meth:`dwopt.dbo._Db.create` method notes.
        * Datetime and reversibility issue see :meth:`dwopt.dbo._Db.write` method notes.

        Args
        ----------
        df : pandas.DataFrame
            Payload Dataframe with data to insert.
        sch_tbl_nme: str
            Table name in form ``my_schema1.my_table1`` or ``my_table1``.

        Examples
        --------
        >>> import pandas as pd
        >>> from dwopt import lt
        >>> tbl = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        >>> lt.drop('test')
        >>> lt.cwrite(tbl, 'test')
        >>> lt.qry('test').run()
           col1 col2
        0     1    a
        1     2    b

        Attempt to write a dataframe into database and query back the same dataframe.

        >>> from dwopt import pg
        >>> from pandas.testing import assert_frame_equal
        >>> df = pg.mtcars(q=1).run().sort_values('name').reset_index(drop=True)
        >>> pg.drop('mtcars2')
        >>> pg.cwrite(df, 'mtcars2')
        >>> df_back = pg.qry('mtcars2').run().sort_values('name').reset_index(drop=True)
        >>> assert_frame_equal(df_back, df)
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
        _logger.info("creating table via sqlalchemy:")
        for col in tbl.columns.items():
            _logger.info(f"{col}")
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
        sch_tbl_nme: str
            Table name in form ``my_schema1.my_table1`` or ``my_table1``.

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
            _logger.info(f"dropping table via sqlalchemy: {sch_tbl_nme}")
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
            _logger.info(f"reflecting table via sqlalchemy: {sch_tbl_nme}")
            self.meta.reflect(self.eng, schema=sch, only=[tbl_nme])
            _logger.info("done")
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
            Default ``iris``.
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
        Uses the postgre `information_schema.constraint_table_usage
        <https://www.postgresql.org/docs/current/infoschema-
        constraint-table-usage.html>`_ table.

        Returns
        -------
        pandas.DataFrame

        Examples
        ----------
        >>> from dwopt import pg
        >>> pg.mtcars()
        >>> pg.add_pkey('mtcars', 'name')
        >>> pg.list_cons().loc[
        ...     lambda x:(x.table_schema == 'public') & (x.table_name == 'mtcars'),
        ...     ['table_name', 'constraint_name']
        ... ]
          table_name constraint_name
        0     mtcars     mtcars_pkey
        """
        if self._dialect == "pg":
            sql = "SELECT * FROM information_schema.constraint_table_usage"
            return self.run(sql)
        else:
            raise NotImplementedError

    def list_tables(self, owner):
        """
        List all tables on database or specified schema.

        Args
        ----------
        owner : str
            Only applicable for oracle. Name of the schema(owner).

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

        Examples
        -----------
        >>> from dwopt import lt
        >>> lt.iris()
        >>> lt.mtcars()
        >>> lt.list_tables().iloc[:,:-1]
            type    name tbl_name  rootpage
        0  table    iris     iris         2
        1  table  mtcars   mtcars         5
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
            Default ``mtcars``.
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
        """Make a :class:`query object <dwopt._qry._Qry>`.

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
        >>> from dwopt import lt
        >>> lt.mtcars()
        >>> lt.qry('mtcars').valc('cyl', 'avg(mpg)')
           cyl   n   avg(mpg)
        0    8  14  15.100000
        1    4  11  26.663636
        2    6   7  19.742857
        """
        return _Qry(self, *args, **kwargs)

    def run(self, sql=None, args=None, pth=None, mods=None, **kwargs):
        """
        Run sql statement.

        Features:

        * Argument binding.
        * Text replacement.
        * Reading from sql script file.

        Args
        ----------
        sql : str, optional
            The sql statement to run.
        args : dict, or [dict], optional
            Dictionary or list of dictionary of argument name str to argument
            data object mappings.
            These argument data objects are passed via sqlalchemy to the database,
            to function as data for the argument names.
            See the notes and the examples section for details.
        pth : str, optional
            Path to sql script, ignored if the sql parameter is not None.
            The script can hold a sql statement, for example a significant piece
            of table creation statement.
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

        An argument name or a modification name is denoted in the sql by prepending
        a colon symbol ``:`` before a series of alphanumeric or underscore symbols.

        In addition, the end of the series for the modification name is to be
        followed by a non-alphanumeric or a end of line symbol. This is to distinguish
        names such as ``:var`` and ``:var1``.

        The args parameter binding is recommanded where possible,
        while the mods paramter method of text replacement gives
        more flexibility when it comes to programatically generate sql statment.

        Examples
        --------
        Run sql:

        >>> from dwopt import lt
        >>> lt.iris()
        >>> lt.run("select * from iris limit 1")
           sepal_length  sepal_width  petal_length  petal_width species
        0           5.1          3.5           1.4          0.2  setosa

        Run sql with argument passing:

        >>> from dwopt import lt
        >>> lt.iris()
        >>> lt.run("select count(1) from iris where species = :x",
        ...     args = {'x':'setosa'})
           count(1)
        0        50

        Run sql with text modification:

        >>> from dwopt import lt
        >>> lt.iris()
        >>> old = 'iris'
        >>> new = 'iris2'
        >>> lt.run("drop table if exists :var", var=new)
        >>> lt.run("create table :x as select * from :y", mods={'x':new, 'y': old})
        >>> lt.run("select count(1) from :tbl", tbl=new)
           count(1)
        0       150

        Run from sql script:

        >>> from dwopt import pg, make_test_tbl
        >>> _ = make_test_tbl(pg)
        >>> pg.run(pth = "E:/projects/my_sql_script.sql",
        ...     my_run_date = '2022-03-03',
        ...     my_label = '20220303',
        ...     threshold = 5)
           count
        0    137

        Above runs the sql stored on ``E:/projects/my_sql_script.sql`` as below:

        .. code-block:: sql

            drop table if exists monthly_extract_:my_label;

            create table monthly_extract_:my_label as
            select * from test
            where
                date = to_date(':my_run_date','YYYY-MM-DD')
                and score > :threshold;

            select count(1) from monthly_extract_:my_label;

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

        Examples
        -----------
        >>> from dwopt import pg
        >>> pg.iris()
        >>> pg.table_cols('public.iris')
            column_name          data_type
        0  sepal_length               real
        1   sepal_width               real
        2  petal_length               real
        3   petal_width               real
        4       species  character varying
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

        **Datetime on sqlite**

        For sqlite tables, datetime columns should be manually converted to str
        and None before insertion.

        **``NaT``**

        Pandas Datetime64 columns are converted into object columns, and the
        ``pandas.NaT`` objects are converted into ``None`` before insertion.

        **``NaN``**

        Pandas Float64 columns are converted into object columns, and the
        ``pandas.NaN`` objects are converted into ``None`` before insertion.

        **Reversibility**

        Ideally python dataframe written to database should allow a exact same
        dataframe to be read back into python. Whether this is true depends on the
        database, data and object types on the dataframe,
        and data types on the database table.

        With the set up used in the :func:`dwopt.make_test_tbl` function, we have
        following results (See the examples and the test function relevant for
        the :meth:`dwopt.dbo._Db.cwrite` method):

        * Postgre example has reversibility except for row ordering and auto generated
          pandas dataframe index. These can be strightened as below.

          .. code-block:: python

              df.sort_values('id').reset_index(drop=True)

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

        * Oracle has same issue as postgre. In addition:
          Datetime milliseconds are lost on the timestamp datatype on database.
          Date are not stored in isoformat, but in dd-MMM-yy format on database.
          Conversion codes:

        .. code-block:: python

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

        Examples
        --------
        Write dataframe into a table.

        >>> import pandas as pd
        >>> from dwopt import lt
        >>> tbl = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        >>> lt.drop('test')
        >>> lt.create('test', col1='int', col2='text')
        >>> lt.write(tbl,'test')
        >>> lt.run('select * from test')
           col1 col2
        0     1    a
        1     2    b

        Attempt to write a dataframe into database and query back the same dataframe.

        >>> from dwopt import make_test_tbl
        >>> from pandas.testing import assert_frame_equal
        >>> pg, df = make_test_tbl('pg')
        >>> pg.drop('test')
        >>> pg.create(
        >>>     "test",
        >>>     dtypes={
        >>>         "id": "bigint primary key",
        >>>         "score": "float8",
        >>>         "amt": "bigint",
        >>>         "cat": "varchar(20)",
        >>>         "date":"date",
        >>>         "time":"timestamp"
        >>>     }
        >>> )
        >>> pg.write(df, 'test')
        >>> df_back = pg.qry('test').run().sort_values('id').reset_index(drop=True)
        >>> assert_frame_equal(df_back, df)
        """
        L = len(df)
        sch_tbl_nme, sch, tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
        if L == 0:
            return
        df = df.copy()
        cols = df.columns.tolist()
        for col in cols:
            if np.issubdtype(df[col].dtype, np.datetime64) or np.issubdtype(
                df[col].dtype, np.float64
            ):
                df[col] = df[col].astype(object).where(~df[col].isna(), None)
        self._remove_sch_tbl(sch_tbl_nme)
        tbl = alc.Table(
            tbl_nme, self.meta, *[alc.Column(col) for col in cols], schema=sch
        )
        _logger.info(f"running:\n{tbl.insert()}")
        _ = df.to_dict("records")
        _logger.info(f"args len={L}, e.g.\n{_[0]}")
        with self.eng.connect() as conn:
            conn.execute(
                tbl.insert(),
                _,
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
    def list_tables(self):
        sql = (
            "select * from sqlite_master "
            "\nwhere type ='table' "
            "\nand name NOT LIKE 'sqlite_%' "
        )
        return self.run(sql)


class Oc(_Db):
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
        sch_tbl_nme, sch, tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
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
