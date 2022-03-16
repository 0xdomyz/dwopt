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
    .. _The query building framework:

    **The query building framework**

    Queries are flexibly built as a combination of a ``sub query``
    and a ``summary query`` from templates.
    Point of ``sub query`` is to be a flexible complementary pre-processing step,
    productivity gain comes from the ``summary query`` templates.

    Example:

    .. code-block:: sql

        -- Sub query: arbituary query within a with clause named x
        with x as (
            select a.*
                ,case when amt < 1000 then amt*1.2 else amt end as amt
            from test a
            where score > 0.5
        )
        -- Summary query: generated from templates
        select
            date,cat
            ,count(1) n
            ,avg(score) avgscore, round(sum(amt)/1e3,2) total
        from x
        group by date,cat
        order by n desc

    Corresponding code::

        from dwopt import lt, make_test_tbl
        _ = make_test_tbl(lt)
        (
            lt.qry('test a')
            .select('a.*').case("amt", "amt < 1000 then amt*1.2", els="amt")
            .where("score > 0.5")
            .valc("date,cat", "avg(score) avgscore, round(sum(amt)/1e3,2) total")
        )

    Use the :ref:`clause methods` to iteratively
    piece together a query, or use the :meth:`~dwopt._qry._Qry.sql` method to provide
    an arbituary query. This created query will then be placed inside a
    with block and become the ``sub query`` on invocation of any :ref:`summary methods`.

    The ``summary query`` is built from parameterized templates via
    the :ref:`summary methods`. Calling one of them completes the whole query
    and immediately runs it.

    This way for large tables, heavy intermediate results from the ``sub query``
    are never realized outside of the database engine,
    while light summary results are placed in python for analysis.

    Examples
    --------

    Create and use qry object using the :meth:`~dwopt.dbo._Db.qry` method from the
    :class:`database operator <dwopt.dbo._Db>` objects::

        from dwopt import lt
        lt.iris()
        lt.qry('iris').len()
        lt.qry('iris').valc('species', 'avg(petal_length)')
        lt.qry('iris').where('petal_length > 2').valc('species', print=1)

    Use the :ref:`summary methods` for analysis::

        from dwopt import pg as d
        d.iris('iris')
        q = d.qry('iris').where('sepal_length > 2.5')
        q.top()
        q.head()
        q.len()
        agg = ', '.join(f'avg({col})' for col in q.cols() if col != 'species')
        q.valc('species', agg)

    Iteratively piece together a query using the :ref:`clause methods`::

        (
            lt.qry('test x')
            .select('x.cat,y.cat as bcat'
                ,'sum(x.score) bscore','sum(y.score) yscore','count(1) n')
            .join("test y","x.id = y.id+1")
            .where('x.id < 1000')
            .group_by('x.cat,y.cat')
            .having('count(1) > 50','sum(y.score) > 100')
            .order_by('x.cat desc','sum(y.score) desc')
            .print()
            #.run()
        )

    .. code-block:: sql

        select x.cat,y.cat as bcat,sum(x.score) bscore,sum(y.score) yscore,
            count(1) n
        from test x
        left join test y
            on x.id = y.id+1
        where x.id < 1000
        group by x.cat,y.cat
        having count(1) > 50
            and sum(y.score) > 100
        order by x.cat desc,sum(y.score) desc

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

    def bin(self):
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
            Treatment str for else clause, default ``NULL``.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.qry("test").case('col',"x>5 then 'A'").print()
            select *
                ,case when x>5 then 'A' else NULL end as col
            from test
        >>> lt.qry("test").case('col',cond = {'x>5':'A','x<2':'B'}).print()
            select *
                ,case
                    when x>5 then A
                    when x<2 then B
                    else NULL
                end as col
            from test
        >>> lt.qry("test").select('a','b') \\
        ... .case('col'
        ...     ,'x<2 then B'
        ...     ,cond = {'x>5':'A'}
        ...     ,els = 'C').print()
            select a,b
                ,case
                    when x<2 then B
                    when x>5 then A
                    else C
                end as col
            from test
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

        Use the presentational out modes:

        >>> from dwopt import pg
        >>> pg.create_schema('test')
        >>> q = pg.iris('test.iris', q=1)
        >>> q.cols(out=1)
        with x as (
            select * from test.iris
        )
        select * from x where 1=2
        >>> q.cols(out=2)
        'with x as (\\n    select * from test.iris\\n)\\nselect * from x where 1=2'

        Select all columns except for one of them:

        >>> from dwopt import lt
        >>> q = lt.iris(q=1)
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
        """
        Add the from clause to query. Alternative to simply specifying table
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
        >>> lt.qry().from_("test").print()
            select * from test
        """
        _ = self.__copy__()
        _._from_ = from_
        return _

    def group_by(self, *args):
        """
        Add the group by clause to query.

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
        >>> lt.qry('test').select('a,count(1) n').group_by('a').print()
            select a,count(1) n
            from test
            group by a
        >>> lt.qry('test').select('a,b,count(1) n').group_by(['a','b']).print()
            select a,b,count(1) n
            from test
            group by a,b
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
        *args : str
            Column names in str. If no value is given, a cols method will be
            performed to fetch the list of all columns, from which a hash will be
            calculated.
        out: int
            Output mode. None to run full query, 1 to print full query,
            2 to return full query as str. Default None.

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
        """
        Add the having clause to query.

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
        >>> lt.qry('test').select('a,count(1) n').group_by('a') \\
        ... .having("count(1)>5").print()
            select a,count(1) n
            from test
            group by a
            having count(1)>5
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

    def join(self, tbl, *args, how="left"):
        """
        Add a join clause to query.
        Calling this method multiple times would add multiple clauses.

        Parameters
        ----------
        tbl : str
            Table name to join to in str format.
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
        >>> lt.qry('test x') \\
        ... .select('x.id','y.id as yid','x.score','z.score as zscore') \\
        ... .join("test y","x.id = y.id+1","x.id <= y.id+1") \\
        ... .join("test z","x.id = z.id+2","x.id >= z.id+1") \\
        ... .print()
            select x.id,y.id as yid,x.score,z.score as zscore
            from test x
            left join test y
                on x.id = y.id+1
                and x.id <= y.id+1
            left join test z
                on x.id = z.id+2
                and x.id >= z.id+1
        """
        _ = self.__copy__()
        on = self._args2str(args, "\n    and ")
        cls = f"{how} join {tbl}" f"\n    on {on}"
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
        >>> lt.qry('test').select('a,count(1) n').group_by('a') \\
        ... .having("count(1)>5").order_by('a','n desc').print()
            select a,count(1) n
            from test
            group by a
            having count(1)>5
            order by a,n desc
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
        >>> lt.qry('test').select("id,score,amt").print()
            select id,score,amt
            from test
        >>> lt.qry('test').select(["id","score","amt"]).print()
            select id,score,amt
            from test
        >>> lt.qry('test').select("id","score","amt").print()
            select id,score,amt
            from test
        """
        _ = self.__copy__()
        _._select = self._args2str(args, sep)
        return _

    def sql(self, sql):
        """
        Replace entire query by specified sql.
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
        >>> lt.qry().sql("select * from test \\nconnect by level <= 5").print()
            select * from test
            connect by level <= 5
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
        if self._ops._dialect == "oc":
            q = "select * from x where rownum<=1"
        else:
            q = "select * from x limit 1"
        if out is None:
            res = self.run(q)
            if res.empty:
                return pd.Series(index=res.columns)
            else:
                return res.iloc[
                    0,
                ]
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
        >>> lt.qry('test').where('x>5','x<10').print()
            select * from test
            where x>5
                and x<10
        >>> lt.qry('test').where(['x>5','x<10','y <> 5']).print()
            select * from test
            where x>5
                and x<10
                and y <> 5
        """
        _ = self.__copy__()
        _._where = self._args2str(args, "\n    and ")
        return _
