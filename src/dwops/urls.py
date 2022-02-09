import sqlalchemy
import keyring
import os
import logging
_logger = logging.getLogger(__name__)

_SERV_ID = f"{os.path.dirname(__file__)}"

def save_url(db_nme,url):
    """
    Use the system keyring service to save database engine url.
    See examples for quick-start.

    A `sqlalchemy engine url <https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls>`_
    combines the user name, password, database names, etc
    into a single string.

    The system keyring service is accessed via the
    `keyring <https://pypi.org/project/keyring/>`_
    package. The service id is the full path to the dwops package files.
    The service on Windows is the Windows Credential Manager.

    Parameters
    ----------
    db_nme : str
        Relevant database code. Either ``pg`` for postgre, ``lt`` for sqlite,
        or ``oc`` for oracle.
    url : str
        Sqlalchemy engine url.

    Returns
    -------
    str
        Message anouncing completion.

    Examples
    --------

    Save connection urls for various databases.

    >>> import dwops
    >>> dwops.save_url('pg'
    ...     ,'postgresql://tiger:scott@localhost/mydatabase')
        'Saved pg url to keyring'
    >>> dwops.save_url('sqlite','sqlite:///foo.db')
        'Saved lt url to keyring'
    >>> dwops.save_url('oracle','oracle://tiger:scott@tnsname')
        'Saved oc url to keyring'

    Exit and re-enter python for it to take effect.

    >>> from dwops import pg, lt, oc
    >>> pg.run('select * from test')
    >>> lt.list_tables()
    >>> oc.qry('dual').head()
    """
    keyring.set_password(_SERV_ID, db_nme, url)
    return f"Saved {db_nme} url to keyring"

def _get_url(db_nme):
    return keyring.get_password(_SERV_ID, db_nme)

def _get_url_default_lt(db_nme):
    '''Make sqlite memory db url if keyring not available'''
    if _get_url(db_nme) is None:
        _logger.info("Keyring not available")
        return 'sqlite://'
    return _get_url(db_nme)

def _save_dummy_url_if_not_exist():
    if _get_url('pg') is None:
        save_url('pg'
            ,'postgresql://scott:tiger@localhost/mydatabase')
    if _get_url('lt') is None:
        save_url('lt','sqlite://')
    if _get_url('oc') is None:
        save_url('oc','oracle://scott:tiger@tnsname')

def make_eng(url):
    """
    Make database connection engine.

    A `sqlalchemy engine url <https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls>`_
    combines the user name, password, database names, etc
    into a single string.
    Engine object best to be created only once per application.

    Parameters
    ----------
    url : str
        Sqlalchemy engine url.

    Returns
    -------
    sqlalchemy engine
    """
    return sqlalchemy.create_engine(url)

def make_meta(eng,schema):
    pass
