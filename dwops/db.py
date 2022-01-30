import sqlalchemy as alc
from sqlalchemy.sql import text
import pandas as pd
import numpy as np
import logging
import re
from dwops._qry import PgQry, LtQry, OcQry
_logger = logging.getLogger(__name__)

def make_eng(url):
    """
    Make database connection engine.
    
    Engine object best to be created once per application.

    Parameters
    ----------
    url : str
        Sqlalchemy engine url. Format varies from database to database
        , typically have user name, password, and location or host address 
        or tns of database.

        Details on:
        https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls

    Returns
    -------
    sqlalchemy engine
    """
    _logger.debug('making sqlalchemy engine')
    return alc.create_engine(url)

def make_meta(eng,schema):
    _logger.debug('making sqlalchemy meta')
    pass

class _Db:
    """
    Generic database operator class.

    This base class should not be instantiated directly by user
    , it's child classes relevant to varies databases 
    should be instantiated and used intead.

    The methods defined here can be grouped into 3 main usages.

    1. Run sql statment.
    2. Run DDL/DML via the convenience methods.
    3. Create query object, which allows running summary query.

    Parameters
    ----------
    eng : sqlalchemy engine
        Database connection engine to be used.

    Attributes
    ----------
    eng : sqlalchemy engine
        Database connection engine to be used.

    See Also
    --------
    dwops.db.Pg : Postgre database operator class.
    dwops.db.Lt : Sqlite database operator class.
    dwops.db.Oc : Oracle database operator class.
    """
    def __init__(self,eng):
        self.eng = eng
        self.meta = {}

    def update_meta(self,meta):
        self.meta.update({meta.schema,meta})

    def run(self,sql=None,args=None,pth=None,mods=None,**kwargs):
        """
        Run sql statement.

        Support argument passing, text replacement 
        and reading statements from sql script.

        Parameters
        ----------
        sql : str, optional
            The sql statement to run. Only 1 statement is allowed.
        args : dict, optional
            Dictionary of argument name str to argument str mappings.
            These arguments are passed to the database, to function as data
            for the argument names. 
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

        An argument name is denoted in the sql by prepending a colon symbol `:` 
        before a str.

        Similiarly, a modification name is denoted by prepending a 
        colon symbol `:` before a str in the sql. 
        The end of str is to be followed by a symbol other than 
        a lower or upper case letter, or a number. 
        It is also ended before a line break.

        The args parameter method of passing arguments is less prone 
        to unintended sql injection, while the mods paramter method of 
        text replacement gives much more flexibility when it comes to
        programatically generate sql statment. For example when database 
        does not support argument passing on DDL/DML statement.

        Examples
        --------
        Make example table.

        >>> import pandas as pd
        >>> from dw import lt
        >>> 
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
        1     2     4

        Run sql with text modification.

        >>> tbl_nme = 'test2'
        >>> lt.run("drop table :tbl",mods = {'tbl':tbl_nme})
        >>> lt.run("create table :tbl as select * from test where :col = 1"
        >>>        , mods = {'tbl':tbl_nme}, col = 'col1')
        >>> lt.run("select *,col2 + 1 as :col from :tbl_nme"
        >>>        , tbl_nme = tbl_nme, col = 'col3')
            col1  col2  col3
        1     1     3     4
        """
        if sql is None and pth is not None:
            with open(pth) as f:
                sql = f.read()
            _logger.info(f'sql from:\n{pth}')
        if mods is not None or len(kwargs) > 0:
            sql = self._bind_mods(sql,mods,**kwargs)
        return self._run(sql,args)

    def _run(self,sql,args = None):
        """Run sql statement with argument passing"""
        with self.eng.begin() as c:
            _logger.info(f'running:\n{sql}')
            if args is not None:
                _logger.info(f'{len(args) = }')
                r = c.execute(text(sql), args)
            else:
                r = c.execute(sql)
            _logger.info('done')
            if r.returns_rows:
                return pd.DataFrame(r.all(),columns = r.keys())

    def _bind_mods(self,sql,mods = None,**kwargs):
        """Apply modification to sql statement"""
        if mods is None:
            mods = kwargs
        else:
            mods.update(kwargs)
        for i,j in mods.items():
            sql = re.sub(f':{i}(?=[^a-zA-Z0-9]|$)',str(j),sql)
            _logger.debug(f'replaced :{i} by {j}')
        return sql

    def create(self,tbl_nme,dtypes = None,**kwargs):
        """Generate and run a create table statment.

        Parameters
        ----------
        tbl_nme : str
            Name of the table to create.
        dtypes : dict, optional
            Dictionary of column names to data types mappings.
        **kwargs :
            Convenient way to add mappings.
            Keyword to argument mappings will be added to the dtypes dictionary.
            The keyword cannot be one of the positional parameter names.

        Notes
        -----

        **Datatypes**

        Datatypes varies across databses, common example below:

        ==========  =======  ===========  ============
        Type        Sqlite   Postgre      Oracle      
        ==========  =======  ===========  ============
        integer     integer  bigint       number
        float       real     float8       float
        string      text     varchar(20)  varchar2(20)
        datetime    text     timestamp    timestamp
        date        text     date         date
        ==========  =======  ===========  ============

        Details:

        * https://www.sqlite.org/datatype3.html
        * https://www.postgresql.org/docs/current/datatype.html
        * https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlqr/Data-Types.html

        **Other statements**

        The dtypes mappings also allow other sql statements which are 
        part of a create statement to be added. For example 
        a primary key constraint.

        Details:

        * https://sqlite.org/lang_createtable.html
        * https://www.postgresql.org/docs/current/sql-createtable.html
        * https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/CREATE-TABLE.html

        Examples
        --------

        Make example table on sqlite.

        >>> from dw import lt
        ... 
        >>> lt.drop('test')
        >>> lt.create('test'
        ...     ,{
        ...         'id':'integer'
        ...         ,'score':'real'
        ...         ,'amt':'integer'
        ...         ,'cat':'text'
        ...         ,'time':'text'
        ...         ,'constraint df_pk':
        ...             'primary key (id)'
        ...     })
        """
        if dtypes is None:
            dtypes = kwargs
        else:
            dtypes.update(kwargs)
        cls = ''
        for col,dtype in dtypes.items():
            cls += f"\n    ,{col} {dtype}"
        self.run(
            f'create table {tbl_nme}('
            f"\n    {cls[6:]}"
            "\n)"
        )

    def write(self,tbl,tbl_nme):
        """

        Parameters
        ----------
        tbl :
            param tbl_nme:
        tbl_nme :
            

        Returns
        -------

        """
        _ = len(tbl)
        _logger.debug(f'writing to {tbl_nme}, len: {_}')
        if _ == 0:
            return
        tbl = tbl.copy()
        cols = tbl.columns.tolist()
        for i in cols:
            if np.issubdtype(tbl[i].dtype, np.datetime64):
                tbl[i] = tbl[i].astype(str)
                logging.debug(f'converted col {i} to str')
        _ = ','.join(f':{i}' for i in cols)
        sql = (
            f"insert into {tbl_nme} ({','.join(cols)})"
            f" values ({_})"
        )
        self.run(sql,args = tbl.to_dict('records'))

    def write_nodup(self,tbl,tbl_nme,pkey,where = None):
        """

        Parameters
        ----------
        tbl :
            param tbl_nme:
        pkey :
            param where:  (Default value = None)
        tbl_nme :
            
        where :
             (Default value = None)

        Returns
        -------

        """
        cols = ','.join(pkey)
        where_cls = f'\nwhere {where}' if where else ''
        db_tbl = self.run(f"select {cols} from {tbl_nme} {where_cls}")
        l_tbl = len(tbl)
        l_db_tbl = len(db_tbl)
        if l_tbl > 0 and l_db_tbl > 0:
            dedup_tbl = (
                tbl
                .merge(db_tbl,how='left',on = pkey
                    ,validate='one_to_one',indicator=True)
                .loc[lambda x:x._merge == 'left_only',:]
                .drop(columns='_merge')
            )
        else:
            dedup_tbl = tbl
        if _logger.isEnabledFor(logging.DEBUG): 
            _logger.debug(
                f"write nodup: {l_tbl = }, {l_db_tbl = }"
                f", {len(dedup_tbl) = }"
            )
        self.write(dedup_tbl,tbl_nme)

    def drop(self,tbl_nme):
        """

        Parameters
        ----------
        tbl_nme :
            

        Returns
        -------

        """
        try:
            self.run(f'drop table {tbl_nme}')
        except Exception as ex:
            _logger.debug(str(ex))
            return f'{tbl_nme} not exist'
        else:
            return f'{tbl_nme} dropped'

    def update(self):
        pass

    def delete(self):
        pass

    def add_pkey(self,tbl_nme,pkey):
        """

        Parameters
        ----------
        tbl_nme :
            param pkey:
        pkey :
            

        Returns
        -------

        """
        sql = f"alter table {tbl_nme} add primary key ({pkey})"
        return self.run(sql)

    def _parse_sch_tbl_nme(self,sch_tbl_nme):
        """Resolve schema dot table name name into components"""
        _ = sch_tbl_nme.split('.')
        n = len(_)
        if n == 1:
            sch = None
            tbl_nme = _[0]
        elif n == 2:
            sch = _[0]
            tbl_nme = _[1]
        else:
            sch = _[0]
            tbl_nme = '.'.join(_[1:])
            tbl_nme = f'"{tbl_nme}"'
        return sch, tbl_nme

class Pg(_Db):
    """
    Postgre databse operator class.

    Inherits most of the methods from the parent class dwops.db._Db.
    Provides prostgre specific methods.

    """
    def list_tables(self):
        """List all tables."""
        sql = (
            "select table_catalog,table_schema,table_name"
            "\n    ,is_insertable_into,commit_action"
            "\nfrom information_schema.tables"
            "\nwhere table_schema"
            "\n   not in ('information_schema','pg_catalog')"
        )
        return self.run(sql)

    def table_cols(self,sch_tbl_nme):
        """
        

        Parameters
        ----------
        sch_tbl_nme : str
            Table name in format: `schema.table`.

        Returns
        -------

        """
        sch,tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
        sql = (
            "select column_name, data_type from information_schema.columns"
            f"\nwhere table_schema = '{sch}' "
            f"\nand table_name = '{tbl_nme}'"
        )
        return self.run(sql)

    def list_cons():
        """ """
        sql = 'SELECT * FROM information_schema.constraint_table_usage'
        return self.run(sql)

    def qry(self,*args,**kwargs):
        """

        Parameters
        ----------
        *args :
            
        **kwargs :
            

        Returns
        -------

        """
        return PgQry(self,*args,**kwargs)

class Lt(_Db):
    """ """
    def list_tables(self):
        """ """
        sql = (
            "select * from sqlite_schema "
            "\nwhere type ='table' "
            "\nand name NOT LIKE 'sqlite_%' "
        )
        return self.run(sql)

    def qry(self,*args,**kwargs):
        """

        Parameters
        ----------
        *args :
            
        **kwargs :
            

        Returns
        -------

        """
        return LtQry(self,*args,**kwargs)

class Oc(_Db):
    """ """
    def list_tables(self,owner):
        """

        Parameters
        ----------
        owner :
            

        Returns
        -------

        """
        sql = (
            "select/*+PARALLEL (4)*/ owner,table_name"
            "\n    ,max(column_name),min(column_name)"
            "\nfrom all_tab_columns"
            f"\nwhere owner = '{owner.upper()}'"
            "\ngroup by owner,table_name"
        )
        return self.run(sql)

    def table_sizes(self):
        """ """
        sql = (
            "select/*+PARALLEL (4)*/"
            "\n    tablespace_name,segment_type,segment_name"
            "\n    ,sum(bytes)/1024/1024 table_size_mb"
            "\nfrom user_extents"
            "\ngroup by tablespace_name,segment_type,segment_name"
        )
        return self.run(sql)

    def table_cols(self,sch_tbl_nme):
        """

        Parameters
        ----------
        sch_tbl_nme :
            

        Returns
        -------

        """
        sch,tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
        sql = (
            "select/*+PARALLEL (4)*/ *"
            "\nfrom all_tab_columns"
            f"\nwhere owner = '{sch.upper()}'"
            f"\nand table_name = '{tbl_nme.upper()}'"
        )
        return self.run(sql)

    def drop(self,tbl_nme):
        """

        Parameters
        ----------
        tbl_nme :
            

        Returns
        -------

        """
        try:
            self.run(f'drop table {tbl_nme} purge')
        except Exception as ex:
            _logger.debug(str(ex))
            return f'{tbl_nme} not exist'
        else:
            return f'{tbl_nme} dropped'

    def qry(self,*args,**kwargs):
        """

        Parameters
        ----------
        *args :
            
        **kwargs :
            

        Returns
        -------

        """
        return OcQry(self,*args,**kwargs)




