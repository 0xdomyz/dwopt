from configparser import ConfigParser as _ConfigParser
from pathlib import Path as _Path
import base64 as _b64
import sqlalchemy as _alc

_PTH = _Path.home().joinpath('.dwops')

def _encode(x):
    return _b64.b64encode(x.encode('UTF-8')).decode('UTF-8')

def _decode(x):
    return _b64.b64decode(x.encode('UTF-8')).decode('UTF-8')

def _get_default_url(db_nme, pth = _PTH):
    cfg = _ConfigParser()
    cfg.read(pth)
    return _decode(cfg.get('url',db_nme))

def _make_wallet_if_not_exist(pth = _PTH):
    cfg = _ConfigParser()
    if not pth.is_file():
        cfg.add_section('url')
        with open(pth, 'w') as f:
            cfg.write(f)
        save_default_url('postgre'
            ,'postgresql://scott:tiger@localhost/mydatabase')
        save_default_url('sqlite','sqlite://')
        save_default_url('oracle','oracle://scott:tiger@tnsname')

def save_default_url(db_nme,url):
    """
    Save obfuscated database engine url to wallet file.

    A database engine url combines the user name, password, database names, etc
    into a single string. See details and syntax for each databases:
    https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls

    See the examples section for usage.

    Parameters
    ----------
    db_nme : str
        Name of database the url is relating to. Either postgre, sqlite,
        or oracle.
    url : str
        Sqlalchemy engine url.

    Returns
    -------
    str
        Message anouncing database name and wallet file location.

    Notes
    -----

    Obfuscation is done via simple base 64 and UTF-8 encoding.
    The Wallet file is automatically created in the user's home directory,
    with the name ``.dwops``. For example: ``C:/Users/scott/.dwops``.

    Consider re-implement these features via modifying the ``urls.py`` file,
    in order to cater to the user's password management strategy.
    For inspirations: https://stackoverflow.com/questions/7014953

    Examples
    --------

    Save connection urls for varies databases.

    >>> import dwops
    >>> dwops.save_default_url('postgre'
    ...     ,'postgresql://tiger:scott@localhost/mydatabase')
        'Saved postgre url to C:\\Users\\scott\\.dwops'
    >>> dwops.save_default_url('sqlite','sqlite:///foo.db')
        'Saved sqlite url to C:\\Users\\scott\\.dwops'
    >>> dwops.save_default_url('oracle','oracle://tiger:scott@tnsname')
        'Saved oracle url to C:\\Users\\scott\\.dwops'

    Exit and re-enter python for it to take effect.

    >>> from dwops import pg, lt, oc
    >>> pg.run('select * from test')
    >>> lt.list_tables()
    >>> oc.qry('dual').head()
    """
    pth = _PTH
    cfg = _ConfigParser()
    cfg.read(pth)
    cfg.set('url',db_nme,_encode(url))
    with open(pth, 'w') as f:
        cfg.write(f)
    return f"Saved {db_nme} url to {pth}"


def make_eng(url):
    """
    Make database connection engine.
    
    Engine object best to be created only once per application.
    See notes for details.

    Parameters
    ----------
    url : str
        Sqlalchemy engine url. Format varies from database to database
        , typically have user name, password, and location or host address 
        or tns of database.

    Returns
    -------
    sqlalchemy engine

    Notes
    -----

    Details on sqlalchemy engine and url syntax:
    https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls
    """
    return _alc.create_engine(url)

def make_meta(eng,schema):
    pass
