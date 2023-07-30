import pandas as pd


class _Qry:
    """
    The query class.

    See examples for quick-start.

    Automatically instantiated via the :meth:`~dwopt.dbo._Db.qry` method from the
    :class:`database operator <dwopt.dbo._Db>` objects.

    It is possible to pass in all needed parameters when calling the method.
    But it is clearer to only pass in one positional argument as table name,
    and use the :ref:`clause methods` to build up a query instead.

    Parameters
    ----------
    operator : dwopt.db._Db
        Database operator object to operate on generated queries.
    from_ : str
        Query table name.
    select: str
        Columns.
    join : str
        Join clause.
    where : str
        Conditions.
    group_by : str
        Group by names.
    having : str
        Conditions.
    order_by : str
        Order by names.
    sql : str
        Sql code.

    Notes
    -----
    .. _The summary query building framework:

    **The summary query building framework**

    Queries are flexibly built as a combination of a ``sub query``
    and a ``summary query`` from templates.
    Point of ``sub query`` is to be a flexible complementary pre-processing step,
    productivity gain comes from the ``summary query`` templates.

    Example:

    .. code-block:: sql

        -- Sub query: arbituary query within a with clause named x
        with x as (
            select * from test
            where score>0.5
                and dte is not null
                and cat is not null
        )
        -- Summary query: generated from templates
        select
            dte, cat
            ,count(1) n
            ,avg(score) avgscore, round(sum(amt)/1e3,2) total
        from x
        group by dte, cat
        order by n desc

    Corresponding code::

        from dwopt import lt, make_test_tbl
        _ = make_test_tbl(lt)
        (
            lt.qry('test').where('score>0.5', 'dte is not null', 'cat is not null')
            .valc(
                'dte, cat',
                'avg(score) avgscore, round(sum(amt)/1e3,2) total',
                out=1
            )
        )

    Use the :ref:`clause methods` to iteratively
    piece together a query, or use the :meth:`~dwopt._qry._Qry.sql` method to provide
    an arbituary query. This created query will then be placed inside a
    with block and become the ``sub query`` on invocation of any summary methods.

    The ``summary query`` is built from parameterized templates via
    the :ref:`summary methods`. Calling one of them completes the whole query
    and immediately runs it.

    This way for large tables, heavy intermediate results from the ``sub query``
    are never realized outside of the database engine,
    while light summary results are placed in python for analysis.

    Examples
    --------

    Create and use qry object using the :meth:`~dwopt.dbo._Db.qry` method from the
    :class:`database operator <dwopt.dbo._Db>` objects:

    >>> from dwopt import lt
    >>> lt.iris()
    >>> lt.qry('iris').len()
    150
    >>> lt.qry('iris').valc('species', 'avg(petal_length)')
       species   n  avg(petal_length)
    0  sicolor  50              4.260
    1   setosa  50              1.462
    2  rginica  50              5.552
    >>> lt.qry('iris').where('petal_length > 2').valc('species', out=1)# doctest: +SKIP
    with x as (
        select * from iris
        where petal_length > 2
    )
    select
        species
        ,count(1) n
    from x
    group by species
    order by n desc

    Iteratively piece together a query using the :ref:`clause methods`:

    >>> from dwopt import lt
    >>> lt.mtcars()
    >>> sql = "select cyl from mtcars group by cyl having count(1) > 10"
    >>> q = (
    ...     lt.qry('mtcars a')
    ...     .select('a.cyl, count(1) n, avg(a.mpg)')
    ...     .case('cat', "a.cyl = 8 then 1", els=0)
    ...     .join(f'({sql}) b', 'a.cyl = b.cyl', how='inner')
    ...     .group_by('a.cyl')
    ...     .having('count(1) > 10')
    ...     .order_by('n desc')
    ... )
    >>>
    >>> q.print()
    select a.cyl, count(1) n, avg(a.mpg)
        ,case when a.cyl = 8 then 1 else 0 end as cat
    from mtcars a
    inner join (select cyl from mtcars group by cyl having count(1) > 10) b
        on a.cyl = b.cyl
    group by a.cyl
    having count(1) > 10
    order by n desc
    >>> q.run()
       cyl   n  avg(a.mpg)  cat
    0    8  14   15.100000    1
    1    4  11   26.663636    0

    Use the :ref:`summary methods` for analysis:

    >>> from dwopt import pg as d
    >>> d.iris('iris')
    >>> q = d.qry('iris').where('petal_length > 2')
    >>> q.top()
    sepal_length        7.0
    sepal_width         3.2
    petal_length        4.7
    petal_width         1.4
    species         sicolor
    Name: 0, dtype: object
    >>> q.head()
       sepal_length  sepal_width  petal_length  petal_width  species
    0           7.0          3.2           4.7          1.4  sicolor
    1           6.4          3.2           4.5          1.5  sicolor
    2           6.9          3.1           4.9          1.5  sicolor
    3           5.5          2.3           4.0          1.3  sicolor
    4           6.5          2.8           4.6          1.5  sicolor
    >>> q.len()
    100
    >>> agg = ', '.join(f'avg({col}) {col}' for col in q.cols() if col != 'species')
    >>> q.valc('species', agg)
       species   n  sepal_length  sepal_width  petal_length  petal_width
    0  sicolor  50         5.936        2.770         4.260        1.326
    1  rginica  50         6.588        2.974         5.552        2.026
    """

    def __init__(
        self,
        operator,
        from_=None,
        select=None,
        join=None,
        where=None,
        group_by=None,
        having=None,
        order_by=None,
        sql=None,
    ):
        self._ops = operator
        self._from_ = from_
        self._select = select
        self._join = join
        self._where = where
        self._group_by = group_by
        self._having = having
        self._order_by = order_by
        self._sql = sql

    def __copy__(self):
        return type(self)(
            self._ops,
            self._from_,
            self._select,
            self._join,
            self._where,
            self._group_by,
            self._having,
            self._order_by,
            self._sql,
        )

    def __str__(self):
        self._make_qry()
        return self._qry

    def _args2str(self, args, sep):
        """
        Parse a tuple of str, or iterator of str,
        into a combined str, fit to be used as part of query.

        Parameters
        ----------
        args : (str,) or ([str],)
            Either a tuple of elemental and/or combined str
            , or a tuple with first and only element
            being a iterator of elemental str.
        sep : str
            Seperator used to seperate elemental str.

        Returns
        -------
        str
            The combined str.

        Examples
        --------
        >>> import dwopt
        >>> dwopt._qry._Qry._args2str(_,('a,b,c',),',')
            'a,b,c'
        >>> dwopt._qry._Qry._args2str(_,('a','b','c',),',')
            'a,b,c'
        >>> dwopt._qry._Qry._args2str(_,(['a','b','c'],),',')
            'a,b,c'
        >>> dwopt._qry._Qry._args2str(_,(('a','b','c'),),',')
            'a,b,c'
        """
        L = len(args)
        if L == 0:
            res = None
        elif L == 1:
            arg = args[0]
            if isinstance(arg, str):
                res = arg
            else:
                res = sep.join(arg)
        else:
            res = sep.join(args)
        return res

    def _make_cls(self, key, load, na=""):
        """Add keyword to clause payload"""
        return f"{key}{load}" if load is not None else na

    def _make_qry(self):
        """Combine query components."""
        if self._sql is not None:
            self._qry = self._sql
        else:
            select = self._make_cls("select ", self._select, "select *")
            from_ = self._make_cls("from ", self._from_, "from test")
            join = self._make_cls("\n", self._join)
            where = self._make_cls("\nwhere ", self._where)
            group_by = self._make_cls("\ngroup by ", self._group_by)
            having = self._make_cls("\nhaving ", self._having)
            order_by = self._make_cls("\norder by ", self._order_by)
            self._qry = (
                select
                + (" " if select == "select *" else "\n")
                + from_
                + join
                + where
                + group_by
                + having
                + order_by
            )
            if self._ops._dialect == "oc":
                self._qry = self._qry.replace("select", "select /*+PARALLEL (4)*/")

    def bin(self, out=None):
        """WIP"""
        raise NotImplementedError

    def case(self, col, *args, cond=None, els="NULL"):
        """Add a case when clause to the select clause.

        Calling this method multiple times would add multiple statements.

        Parameters
        ----------
        col : str
            Column name of the resulting column.
        *args : str
            Positional argument in form 'condition then treatement'.
        cond : dict
            Dictionary of condition str to treatment str mappings.
        els : str
            Value for else clause, default ``NULL``.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.iris()
        >>> q = lt.qry('iris').case('mt4',"petal_length > 4 then 1", els=0)
        >>> q.print()
        select *
            ,case when petal_length > 4 then 1 else 0 end as mt4
        from iris
        >>> q.valc('species','avg(mt4) pct_mt4')
           species   n  pct_mt4
        0  sicolor  50     0.68
        1   setosa  50     0.00
        2  rginica  50     1.00

        Combine case when, value counts, and pivot:

        >>> from dwopt import lt as d
        >>> d.iris(q=1).case('cat', # doctest: +SKIP
        ...     "petal_length > 5             then '5+'",
        ...     "petal_length between 2 and 5 then '2-5'",
        ...     "petal_length < 2             then '-2'",
        ... ).valc('species, cat').pivot('species','cat','n')
        cat        -2   2-5    5+
        species
        rginica   NaN   9.0  41.0
        setosa   50.0   NaN   NaN
        sicolor   NaN  49.0   1.0

        Use the ``cond`` argument:

        >>> from dwopt import pg
        >>> pg.mtcars()
        >>> pg.qry("mtcars").select('name, mpg').case('cat', cond = {
        ...         'mpg between 10 and 20':15,
        ...         'mpg between 20 and 30':25,
        ...         'mpg between 30 and 40':35,
        ...     }
        ... ).print()
        select name, mpg
            ,case
                when mpg between 10 and 20 then 15
                when mpg between 20 and 30 then 25
                when mpg between 30 and 40 then 35
                else NULL
            end as cat
        from mtcars
        """
        _ = self.__copy__()
        if cond is not None:
            for i, j in cond.items():
                args = args + (f"{i} then {j}",)
        if len(args) == 0:
            raise TypeError(
                "qry.case() takes at least one args or cond argument (0 given)"
            )
        elif len(args) == 1 and len(args[0]) < 35:
            cls = f"\n    ,case when {args[0]} else {els} end as {col}"
        else:
            cls = self._args2str(args, "\n        when ")
            cls = (
                "\n    ,case"
                f"\n        when {cls}"
                f"\n        else {els}"
                f"\n    end as {col}"
            )
        if _._select is None:
            _._select = "*" + cls
        else:
            _._select = _._select + cls
        return _

    def cols(self, out=None):
        """Fetch column names of the sub query table.

        Args
        --------
        out: int
            Output mode. None to run full query, 1 to print full query,
            2 to return full query as str. Default None.

        Returns
        -------
        Column names as list of str. Or full query as str.

        Examples
        --------
        >>> from dwopt import lt as d
        >>> d.iris()
        >>> d.qry('iris').cols()
        ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']
        >>> d.qry('iris').cols(out=1)
        with x as (
            select * from iris
        )
        select * from x where 1=2

        Use with comprehension to obtain subsets:

        >>> from dwopt import pg as d
        >>> d.create_schema('test')
        >>> q = d.iris('test.iris', q=1)
        >>> [i for i in q.cols() if i != 'species']
        ['sepal_length', 'sepal_width', 'petal_length', 'petal_width']
        """
        q = "select * from x where 1=2"
        if out is None:
            return self.run(q).columns.tolist()
        elif out == 1:
            self.print(q)
        elif out == 2:
            return self.str(q)
        else:
            raise ValueError("Invalid out, one of: None, 1, 2")

    def dist(self, *args, out=None):
        """Count number of distinct occurances of data.

        Works on specified columns, or combination of columns, of the sub query table.

        Args
        ----------
        *args : str or [str]
            Either column names as str, or iterator of column name str.
        out: int
            Output mode. None to run full query, 1 to print full query,
            2 to return full query as str. Default None.

        Returns
        -------
        pandas.Series

        Examples
        --------
        >>> from dwopt import lt
        >>> q = lt.mtcars(q=1)
        >>> q.dist('mpg')
        count(distinct mpg)    25
        Name: 0, dtype: int64
        >>> q.dist('mpg', 'cyl')
        count(distinct mpg)    25
        count(distinct cyl)     3
        Name: 0, dtype: int64
        >>> q.dist(['mpg', 'cyl'])
        count(distinct mpg || '_' || cyl)    27
        Name: 0, dtype: int64
        >>> q.dist(*q.cols()[:4])
        count(distinct name)    32
        count(distinct mpg)     25
        count(distinct cyl)      3
        count(distinct disp)    27
        Name: 0, dtype: int64

        >>> from dwopt import pg
        >>> pg.create_schema('test')
        >>> q = pg.mtcars('test.mtcars', q=1)
        >>> q.dist('mpg', ['mpg', 'cyl'], out=1) # doctest: +SKIP
        with x as (
            select * from test.mtcars
        )
        select
            count(distinct mpg)
            ,count(distinct mpg || '_' || cyl)
        from x
        >>> q.dist('mpg', ['mpg', 'cyl'])
        count    25
        count    27
        Name: 0, dtype: int64
        """
        _ = (" || '_' || ".join(_) if not isinstance(_, str) else _ for _ in args)
        _ = "".join(
            f"    ,count(distinct {j})\n" if i else f"    count(distinct {j})\n"
            for i, j in enumerate(_)
        )
        q = "select \n" f"{_}" "from x"

        if out is None:
            return self.run(q).iloc[0, :]
        elif out == 1:
            self.print(q)
        elif out == 2:
            return self.str(q)
        else:
            raise ValueError("Invalid out, one of: None, 1, 2")

    def five(self, out=None):
        """WIP"""
        raise NotImplementedError

    def from_(self, from_):
        """Add the from clause to query.

        Alternative to simply specifying table
        name as the only argument of the qry method. Use the qry method.

        Parameters
        ----------
        from_ : str
            Table name str.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.iris()
        >>> lt.qry().from_("iris").print()
        select * from iris
        """
        _ = self.__copy__()
        _._from_ = from_
        return _

    def group_by(self, *args):
        """Add the group by clause to query.

        Parameters
        ----------
        *args : str or [str]
            Group by columns in str format, or iterator of them.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.mtcars()
        >>> (
        ...     lt.qry('mtcars')
        ...     .select('hp, count(1) n, avg(mpg)')
        ...     .group_by('hp')
        ...     .having("avg(mpg) > 30")
        ...     .run()
        ... )
            hp  n  avg(mpg)
        0   52  1      30.4
        1   65  1      33.9
        2  113  1      30.4
        >>> (
        ...     lt.qry('mtcars')
        ...     .select('hp, cyl, count(1) n, avg(mpg)')
        ...     .group_by('hp,cyl')
        ...     #.group_by('hp', 'cyl') #same effect
        ...     #.group_by(['hp', 'cyl']) #same effect
        ...     .having("avg(mpg) > 30")
        ...     .print()
        ... )
        select hp, cyl, count(1) n, avg(mpg)
        from mtcars
        group by hp,cyl
        having avg(mpg) > 30
        """
        _ = self.__copy__()
        _._group_by = self._args2str(args, ",")
        return _

    def hash(self, *args, out=None):
        """Calculate a simple oracle hash for table.

        Arrive at a indicative hash value for a number of columns or all columns of
        a sub query table.
        Hash value is a number or symbol that is calculated from data
        , and is sensitive to any small changes in data. It serves as method to
        detect if any data element in data is changed.

        Args
        ----------
        *args : str or [str]
            Column names in str. If no value is given, a cols method will be
            performed to fetch the list of all columns, from which a hash will be
            calculated.
            Also allow passing in a single list of str column names.
        out: int
            Output mode. None to run full query, 1 to print full query,
            2 to return full query as str. Default None.

        Returns
        -------
        int

        Examples
        --------
        ::

            from dwopt import oc
            q = oc.iris(q=1)
            q.hash('petal_length')
            q.hash('petal_length', 'petal_width')
            q.hash(['petal_length', 'petal_width'])
            q.hash(out=1)
        """
        if self._ops._dialect != "oc":
            raise NotImplementedError
        if len(args) == 0:
            args = self.cols()
        _ = args[0] if len(args) == 1 and not isinstance(args[0], str) else args
        _ = " || '_' || ".join(_)
        q = (
            "select/*+ PARALLEL(4) */ \n"
            "    ora_hash(sum(ora_hash(\n"
            f"        {_}\n"
            "    ) - 4294967296/2)) hash\n"
            "from x"
        )
        if out is None:
            return self.run(q).iloc[0, 0]
        elif out == 1:
            self.print(q)
        elif out == 2:
            return self.str(q)
        else:
            raise ValueError("Invalid out, one of: None, 1, 2")

    def having(self, *args):
        """Add the having clause to query.

        Parameters
        ----------
        *args : str or [str]
            Conditions in str format, or iterator of them.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.mtcars()
        >>> q = (
        ...     lt.qry('mtcars')
        ...     .select('hp, count(1) n, avg(mpg)')
        ...     .group_by('hp')
        ...     .having("count(1) > 1", "avg(mpg) > 15")
        ...     .order_by('n desc')
        ... )
        >>>
        >>> q.print()
        select hp, count(1) n, avg(mpg)
        from mtcars
        group by hp
        having count(1) > 1
            and avg(mpg) > 15
        order by n desc
        >>> q.run()
            hp  n   avg(mpg)
        0  180  3  16.300000
        1  175  3  19.200000
        2  110  3  21.133333
        3  150  2  15.350000
        4  123  2  18.500000
        5   66  2  29.850000
        """
        _ = self.__copy__()
        _._having = self._args2str(args, "\n    and ")
        return _

    def head(self, out=None):
        """Fetch top 5 rows of the sub query table.

        Args
        -------
        out: int
            Output mode. None to run full query, 1 to print full query,
            2 to return full query as str. Default None.

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.iris()
        >>> lt.qry("iris").head()
           sepal_length  sepal_width  petal_length  petal_width species
        0           5.1          3.5           1.4          0.2  setosa
        1           4.9          3.0           1.4          0.2  setosa
        2           4.7          3.2           1.3          0.2  setosa
        3           4.6          3.1           1.5          0.2  setosa
        4           5.0          3.6           1.4          0.2  setosa
        """
        if self._ops._dialect == "oc":
            q = "select * from x where rownum <= 5"
        else:
            q = "select * from x limit 5"
        if out is None:
            return self.run(q)
        elif out == 1:
            self.print(q)
        elif out == 2:
            return self.str(q)
        else:
            raise ValueError("Invalid out, one of: None, 1, 2")

    def join(self, sch_tbl_nme, *args, how="left"):
        """Add a join clause to query.

        Calling this method multiple times would add multiple clauses.

        Parameters
        ----------
        sch_tbl_nme : str
            Table name in form ``my_schema1.my_table1`` or ``my_table1``.
        *args : str
            Joining conditions in str format.
        how : str
            The join keyword in str format, for example: ``inner``, ``cross``.
            Default ``left``.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.mtcars()
        >>> q = (
        ...     lt.qry('mtcars a').select('a.cyl, b.mpg')
        ...     .join('mtcars b','a.name = b.name')
        ... )
        >>> q.valc('cyl', 'avg(mpg)')
           cyl   n   avg(mpg)
        0    8  14  15.100000
        1    4  11  26.663636
        2    6   7  19.742857
        >>> q.valc('cyl', 'avg(mpg)', out=1) # doctest: +SKIP
        with x as (
            select a.cyl, b.mpg
            from mtcars a
            left join mtcars b
                on a.name = b.name
        )
        select
            cyl
            ,count(1) n
            ,avg(mpg)
        from x
        group by cyl
        order by n desc

        >>> (
        ...     lt.qry('mtcars a').select('a.cyl, b.mpg, c.hp')
        ...     .join('mtcars b','a.name = b.name')
        ...     .join('mtcars c','a.name = c.name')
        ...     .valc('cyl', 'avg(mpg), avg(hp)')
        ... )
           cyl   n   avg(mpg)     avg(hp)
        0    8  14  15.100000  209.214286
        1    4  11  26.663636   82.636364
        2    6   7  19.742857  122.285714

        Inject a sub-query into the ``sch_tbl_nme`` argument:

        >>> sql = "select cyl from mtcars group by cyl having count(1) > 10"
        >>> q = (
        ...     lt.qry('mtcars a').select('a.cyl, a.mpg')
        ...     .join(f'({sql}) b', 'a.cyl = b.cyl', how='inner')
        ... )
        >>> q.valc('cyl', 'avg(mpg)')
           cyl   n   avg(mpg)
        0    8  14  15.100000
        1    4  11  26.663636
        >>> q.valc('cyl', 'avg(mpg)', out=1) # doctest: +SKIP
        with x as (
            select a.cyl, a.mpg
            from mtcars a
            inner join (select cyl from mtcars group by cyl having count(1) > 10) b
                on a.cyl = b.cyl
        )
        select
            cyl
            ,count(1) n
            ,avg(mpg)
        from x
        group by cyl
        order by n desc
        """
        _ = self.__copy__()
        on = self._args2str(args, "\n    and ")
        cls = f"{how} join {sch_tbl_nme}" f"\n    on {on}"
        if _._join is not None:
            _._join = _._join + "\n" + cls
        else:
            _._join = cls
        return _

    def len(self, out=None):
        """Length of the sub query table.

        Args
        -------
        out: int
            Output mode. None to run full query, 1 to print full query,
            2 to return full query as str. Default None.

        Returns
        -------
        int

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.iris()
        >>> lt.qry('iris').len()
        150
        >>> lt.mtcars(q=1).len()
        32
        """
        q = "select count(1) from x"
        if out is None:
            return self.run(q).iloc[0, 0]
        elif out == 1:
            self.print(q)
        elif out == 2:
            return self.str(q)
        else:
            raise ValueError("Invalid out, one of: None, 1, 2")

    def mimx(self, col, out=None):
        """Fetch maximum and minimum values of a column.

        Args
        ----------
        col : str
            Column name as str.
        out: int
            Output mode. None to run full query, 1 to print full query,
            2 to return full query as str. Default None.

        Returns
        -------
        pandas.Series

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.iris()
        >>> lt.qry('iris').mimx('sepal_width')
        max(sepal_width)    4.4
        min(sepal_width)    2.0
        Name: 0, dtype: float64
        """
        q = "select \n" f"    max({col}),min({col})\n" "from x"
        if out is None:
            return self.run(q).iloc[0, :]
        elif out == 1:
            self.print(q)
        elif out == 2:
            return self.str(q)
        else:
            raise ValueError("Invalid out, one of: None, 1, 2")

    def order_by(self, *args):
        """
        Add the order by clause to query.

        Parameters
        ----------
        *args : str or [str]
            Order by names in str format, or iterator of them.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.mtcars()
        >>> q = (
        ...     lt.qry('mtcars')
        ...     .select('hp, count(1) n, avg(mpg)')
        ...     .group_by('hp')
        ...     .having("count(1) > 1", "avg(mpg) > 15")
        ...     .order_by('n desc')
        ... )
        >>>
        >>> q.run()
            hp  n   avg(mpg)
        0  180  3  16.300000
        1  175  3  19.200000
        2  110  3  21.133333
        3  150  2  15.350000
        4  123  2  18.500000
        5   66  2  29.850000
        """
        _ = self.__copy__()
        _._order_by = self._args2str(args, ",")
        return _

    def pct(self):
        """WIP"""
        raise NotImplementedError

    def piv(self):
        """WIP"""
        raise NotImplementedError

    def print(self, sum_qry=None):
        """Print the built query.

        * If the ``sum_qry`` argument is not given value,
          print underlying query as it is.
        * If a value is given, enclose the underlying query into a with
          clause, attach the summary query after it, print the full query.

        See also
        ---------
        :meth:`dwopt._qry._Qry.str`
        :meth:`dwopt._qry._Qry.run`

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.iris()
        >>> lt.qry('iris').select('count(1)').print()
        select count(1)
        from iris
        >>> lt.qry('iris').print('select count(1) from x')
        with x as (
            select * from iris
        )
        select count(1) from x
        """
        print(self.str(sum_qry))

    def run(self, sum_qry=None, **kwargs):
        """Run the built query.

        * If the ``sum_qry`` argument is not given value,
          run underlying query as it is.
        * If a value is given, enclose the underlying query into a with
          clause, attach the summary query after it, run the full query.

        Args
        ----------
        sum_qry: str
            Summary query string.
        **kwargs:
            Keyword arguments to pass on to database operator's run method.

        Returns
        -------
        pandas.DataFrame or None
            Returned by the database operator's
            :meth:`dwopt.dbo._Db.run` method.

        See also
        ---------
        :meth:`dwopt._qry._Qry.str`
        :meth:`dwopt._qry._Qry.print`

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.iris()
        >>> lt.qry('iris').select('count(1)').run()
           count(1)
        0       150
        >>> lt.qry('iris').run('select count(1) from x')
           count(1)
        0       150
        >>> lt.qry('iris').where('petal_length>:x').run(
        ...     'select count(1) from x', mods={'x':2}
        ... )
           count(1)
        0       100
        """
        qry = self.str(sum_qry)
        return self._ops.run(qry, **kwargs)

    def select(self, *args, sep=","):
        """
        Add the select clause to query.

        Parameters
        ----------
        *args : str or [str]
            Column name str as positional arguments
            , or an iterator of str column names.
        sep : str
            Symbol used for seperating column names, default ``,``.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> q = lt.iris(q=1)
        >>> q.select("species, sepal_length").print()
        select species, sepal_length
        from iris
        >>> q.select("species", "sepal_length").print()
        select species,sepal_length
        from iris
        >>> q.select(["species", "sepal_length"]).print()
        select species,sepal_length
        from iris
        >>> q.select(["species", "sepal_length + 2 as sepal_length_new"]).print()
        select species,sepal_length + 2 as sepal_length_new
        from iris
        """
        _ = self.__copy__()
        _._select = self._args2str(args, sep)
        return _

    def sql(self, sql):
        """Replace the underlying query by specified sql.

        This allows arbituary advanced sql to be incorporated into framework.

        Parameters
        ----------
        sql : str
            Sql code in str format.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.iris()
        >>> q = lt.qry().sql("select * from iris where sepal_length > 7")
        >>> q.len()
        12
        >>> q.print()
        select * from iris where sepal_length > 7
        """
        _ = self.__copy__()
        _._sql = sql
        return _

    def str(self, sum_qry=None):
        """Return the built query as str.

        * If the ``sum_qry`` argument is not given value,
          return underlying query as it is.
        * If a value is given, enclose the underlying query into a with
          clause, attach the summary query after it, return the full query.

        Args
        ----------
        sum_qry: str
            Summary query string.

        Returns
        ---------
        str

        See also
        ---------
        :meth:`dwopt._qry._Qry.print`
        :meth:`dwopt._qry._Qry.run`

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.iris()
        >>> lt.qry('iris').select('count(1)').str()
        'select count(1)\\nfrom iris'
        >>> lt.qry('iris').str('select count(1) from x')
        'with x as (\\n    select * from iris\\n)\\nselect count(1) from x'
        """
        self._make_qry()
        if sum_qry is not None:
            _ = self._qry.replace("\n", "\n    ")
            _ = "with x as (\n" f"    {_}\n" ")"
            qry = f"{_}\n{sum_qry}"
        else:
            qry = self._qry
        return qry

    def top(self, out=None):
        """Fetch top row of the sub query table.

        Args
        -------
        out: int
            Output mode. None to run full query, 1 to print full query,
            2 to return full query as str. Default None.

        Returns
        -------
        pandas.Series

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.mtcars()
        >>> lt.qry('mtcars').top()
        name    Mazda RX4
        mpg          21.0
        cyl             6
        disp        160.0
        hp            110
        drat          3.9
        wt           2.62
        qsec        16.46
        vs              0
        am              1
        gear            4
        carb            4
        Name: 0, dtype: object

        Use ``print`` and ``to_string`` to force display of all columns:

        >>> print(lt.iris(q=1).top().to_string())
        sepal_length       5.1
        sepal_width        3.5
        petal_length       1.4
        petal_width        0.2
        species         setosa
        """
        if self._ops._dialect == "oc":
            q = "select * from x where rownum<=1"
        else:
            q = "select * from x limit 1"
        if out is None:
            res = self.run(q)
            if res.empty:
                return pd.Series(index=res.columns)
            else:
                return res.iloc[0,]
        elif out == 1:
            self.print(q)
        elif out == 2:
            return self.str(q)
        else:
            raise ValueError("Invalid out, one of: None, 1, 2")

    def valc(self, group_by, agg=None, order_by=None, n=True, out=None):
        """Value count of a column or combination of columns.

        A value count is a
        group by query, with total number of row of each group calculated.
        Also allow custom summary calculation, and custom order by clauses
        to be added.

        Args
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
        out: int
            Output mode. None to run full query, 1 to print full query,
            2 to return full query as str. Default None.

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        Various configuration of value counts:

        >>> from dwopt import lt
        >>> lt.iris()
        >>> lt.qry('iris').valc('species', 'avg(petal_length)')
           species   n  avg(petal_length)
        0  sicolor  50              4.260
        1   setosa  50              1.462
        2  rginica  50              5.552
        >>> lt.qry('iris').valc('species', 'avg(petal_length) avg', 'avg desc')
           species   n    avg
        0  rginica  50  5.552
        1  sicolor  50  4.260
        2   setosa  50  1.462
        >>> lt.qry('iris').valc('species', 'avg(petal_length)', n=False)
           species  avg(petal_length)
        0  rginica              5.552
        1   setosa              1.462
        2  sicolor              4.260
        >>> lt.qry('iris').valc('species', 'avg(petal_length)', out=1)# doctest: +SKIP
        with x as (
            select * from iris
        )
        select
            species
            ,count(1) n
            ,avg(petal_length)
        from x
        group by species
        order by n desc

        Excel-pivot-table-like API:

        .. code-block :: python

            from dwopt import lt, make_test_tbl
            import logging
            # logging.basicConfig(level = logging.INFO)
            _ = make_test_tbl(lt)
            (
                lt.qry('test').where('score>0.5', 'dte is not null', 'cat is not null')
                .valc('dte, cat', 'avg(score) avgscore, round(sum(amt)/1e3,2) total')
                .pivot('dte', 'cat')
            )

        Logs showing sql used:

        .. code-block :: sql

            INFO:dwopt.dbo:running:
            with x as (
                select * from test
                where score>0.5
                    and dte is not null
                    and cat is not null
            )
            select
                dte, cat
                ,count(1) n
                ,avg(score) avgscore, round(sum(amt)/1e3,2) total
            from x
            group by dte, cat
            order by n desc
            INFO:dwopt.dbo:done

        Results::

                           n        avgscore             total
            cat         test train      test     train    test   train
            dte
            2022-01-01  1140  1051  2.736275  2.800106  565.67  530.09
            2022-02-02  1077  1100  2.759061  2.748898  536.68  544.10
            2022-03-03  1037  1072  2.728527  2.743825  521.54  528.85

        Combine case when, value counts, and pivot:

        >>> from dwopt import lt as d
        >>> d.iris(q=1).case('cat', # doctest: +SKIP
        ...     "petal_length > 5             then '5+'",
        ...     "petal_length between 2 and 5 then '2-5'",
        ...     "petal_length < 2             then '-2'",
        ... ).valc('species, cat').pivot('species','cat','n')
        cat        -2   2-5    5+
        species
        rginica   NaN   9.0  41.0
        setosa   50.0   NaN   NaN
        sicolor   NaN  49.0   1.0

        Make summary for all relevant columns via generated statements:

        >>> from dwopt import lt
        >>> lt.iris()
        >>> q = lt.qry('iris')
        >>> agg = ', '.join(f'avg({col}) {col}' for col in q.cols() if col != 'species')
        >>> q.valc('species', agg)
           species   n  sepal_length  sepal_width  petal_length  petal_width
        0  sicolor  50         5.936        2.770         4.260        1.326
        1   setosa  50         5.006        3.428         1.462        0.246
        2  rginica  50         6.588        2.974         5.552        2.026
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
        q = (
            "select \n"
            f"    {group_by_cls}\n"
            f"{f'    ,count(1) n{chr(10)}' if n else ''}"
            f"{agg_cls}"
            "from x\n"
            f"group by {group_by_cls}\n"
            f"order by {order_by_cls}"
        )
        if out is None:
            return self.run(q)
        elif out == 1:
            self.print(q)
        elif out == 2:
            return self.str(q)
        else:
            raise ValueError("Invalid out, one of: None, 1, 2")

    def where(self, *args):
        """
        Add the where clause to query.

        Parameters
        ----------
        *args : str or [str]
            Conditions in str format, or iterator of condition str.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.mtcars()
        >>> lt.qry('mtcars').where("cyl = 4").len()
        11
        >>> lt.qry('mtcars').where("cyl = 4", "vs = 0").run()
                    name   mpg  cyl   disp  hp  drat    wt  qsec  vs  am  gear  carb
        0  Porsche 914-2  26.0    4  120.3  91  4.43  2.14  16.7   0   1     5     2
        >>> lt.qry('mtcars').where("cyl = 4 and vs = 0").print()
        select * from mtcars
        where cyl = 4 and vs = 0
        """
        _ = self.__copy__()
        _._where = self._args2str(args, "\n    and ")
        return _
