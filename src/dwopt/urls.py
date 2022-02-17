import keyring
import os
from pathlib import Path
from configparser import ConfigParser
import sqlalchemy
import logging

_logger = logging.getLogger(__name__)
_CONFIG_PTH = Path.home() / 'dwopt'
_KEYRING_SERV_ID = f"{Path(__file__).parent.resolve().as_posix()}"

def save_url(db_nme, url, method = 'keyring'):
    """
    Save database engine url to system in one of 3 methods.
    See examples for quick-start.

    A `sqlalchemy engine url <https://docs.sqlalchemy.org/en/14/core/
    engines.html#database-urls>`_
    combines the user name, password, database names, etc
    into a single string.

    On package import, default url are taken fristly from keyring if available,
    then environment variable if available, then the config file if available,
    lastly a set of dummy urls shown in the examples section.

    The system keyring service is accessed via the
    `keyring <https://pypi.org/project/keyring/>`_
    package. The service id is the full path to the dwopt package files.
    The service on Windows is the Windows Credential Manager.

    The config file is created with name .dwopt on the system HOME directory.

    Parameters
    ----------
    db_nme : str
        Relevant database code. Either ``pg`` for postgre, ``lt`` for sqlite,
        or ``oc`` for oracle.
    url : str
        Sqlalchemy engine url.
    method: str
        Method used to save, either 'keyring', 'environ' or 'config'.
        Default 'keyring'.

    Returns
    -------
    str
        Message anouncing completion.

    Examples
    --------

    Save connection urls in various methods for various databases.

    >>> import dwopt
    >>> dwopt.save_url('pg'
    ...     ,'postgresql://scott:tiger@localhost/mydatabase')
        'Saved pg url to keyring'
    >>> dwopt.save_url('sqlite','sqlite://','environ')
        'Saved lt url to environ'
    >>> dwopt.save_url('oracle','oracle://scott:tiger@tnsname','config')
        'Saved oc url to config'

    Exit and re-enter python for it to take effect.

    >>> from dwopt import pg, lt, oc
    >>> pg.run('select * from test')
    >>> lt.list_tables()
    >>> oc.qry('dual').head()
    """
    if method == 'keyring':
        keyring.set_password(_KEYRING_SERV_ID, db_nme, url)
    elif method == 'environ':
        os.environ[f"dwopt_{db_nme}"] = url
    elif method == 'config':
        cfg = ConfigParser()
        cfg.read(_CONFIG_PTH)
        if not cfg.has_section('url'):
            cfg.add_section('url')
        cfg.set('url', db_nme, url)
        with open(_CONFIG_PTH,'w') as f:
            cfg.write(f)
    else:
        raise Exception('Invalid method')
    return f"Saved {db_nme} url to {method}"

def _get_url(db_nme):
    """Get url if possible, else dummy url."""
    url = None

    try:
        url = keyring.get_password(_KEYRING_SERV_ID, db_nme)
    except Exception as e:
        _logger.warn(e)
    if url is not None:
        return url

    url = os.environ.get(f"dwopt_{db_nme}")
    if url is not None:
        return url

    cfg = ConfigParser()
    cfg.read(_CONFIG_PTH)
    if cfg.has_option('url',db_nme):
        url = cfg.get('url',db_nme)
    if url is not None:
        return url

    if db_nme == 'pg':
        url = 'postgresql://scott:tiger@localhost/mydatabase'
    elif db_nme == 'lt':
        url = 'sqlite://'
    elif db_nme == 'oc':
        url = 'oracle://scott:tiger@tnsname'
    else:
        raise Exception("Invalid db_nme")
    return url

def make_eng(url):
    """
    Make database connection engine.

    A `sqlalchemy engine url <https://docs.sqlalchemy.org/en/14/
    core/engines.html#database-urls>`_
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
