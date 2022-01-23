import sqlalchemy as alc
from sqlalchemy.sql import text
import pandas as pd
import numpy as np
import logging
import re
from dw._qry import PgQry, LtQry, OcQry
_logger = logging.getLogger(__name__)

def make_eng(url):
    """Return sqlalchemy engine from database connection url

    :param url: 

    """
    _logger.debug('making sqlalchemy engine')
    return alc.create_engine(url)

def make_meta(eng,schema):
    _logger.debug('making sqlalchemy meta')
    pass

class _Db:
    """ Base class for different database operator classes. 
    Database operators provide methods to run statement, construct and run DDL
    , DML statements, provide access to the query objects

    :param eng: sqlalchemy engine
    """
    def __init__(self,eng):
        self.eng = eng
        self.meta = {}

    def update_meta(self,meta):
        self.meta.update({meta.schema,meta})

    def run(self,sql=None,args=None,pth=None,mods=None,**kwargs):
        """Run sql statement. Support text replacement and running from 
        sql script.

        :param sql:  (Default value = None)
        :type sql: str, optional
        :param args:  (Default value = None)
        :type args: Dictionary of argument name to argument str mappings
            , optional
        :param pth: path to sql script (Default value = None)
        :type pth: str, optional
        :param mods:  (Default value = None)
        :type mods: Dictionary of modification name to modification str mappings
            ,optional
        :param **kwargs: keyword arguments that will be interpreted as
            modification name to modification mappings

        """
        if sql is None and pth is not None:
            with open(pth) as f:
                sql = f.read()
            _logger.info(f'sql from:\n{pth}')
        if mods is not None or len(kwargs) > 0:
            sql = self._bind_mods(sql,mods,**kwargs)
        return self._run(sql,args)

    def _run(self,sql,args = None):
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
        if mods is None:
            mods = kwargs
        else:
            mods.update(kwargs)
        for i,j in mods.items():
            sql = re.sub(f':{i}(?=[^a-zA-Z0-9]|$)',str(j),sql)
            _logger.debug(f'replaced :{i} by {j}')
        return sql

    def create(self,tbl_nme,dtypes = None,**kwargs):
        """Make and run create table statment

        :param tbl_nme: 
        :param dtypes: dictionary of column names (Default value = None)
        :param **kwargs: 

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

        :param tbl: 
        :param tbl_nme: 

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

        :param tbl: 
        :param tbl_nme: 
        :param pkey: 
        :param where:  (Default value = None)

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

        :param tbl_nme: 

        """
        try:
            self.run(f'drop table {tbl_nme}')
        except Exception as ex:
            _logger.debug(str(ex))
            return f'{tbl_nme} not exist'
        else:
            return f'{tbl_nme} dropped'

    def update(self):
        """ """
        pass

    def delete(self):
        """ """
        pass

    def add_pkey(self,tbl_nme,pkey):
        """

        :param tbl_nme: 
        :param pkey: 

        """
        sql = f"alter table {tbl_nme} add primary key ({pkey})"
        return self.run(sql)

    def _parse_sch_tbl_nme(self,sch_tbl_nme):
        """

        :param sch_tbl_nme: 

        """
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
    """ """
    def list_tables(self):
        """ """
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

        :param sch_tbl_nme: 

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

        :param *args: 
        :param **kwargs: 

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

        :param *args: 
        :param **kwargs: 

        """
        return LtQry(self,*args,**kwargs)

class Oc(_Db):
    """ """
    def list_tables(self,owner):
        """

        :param owner: 

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

        :param sch_tbl_nme: 

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

        :param tbl_nme: 

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

        :param *args: 
        :param **kwargs: 

        """
        return OcQry(self,*args,**kwargs)




