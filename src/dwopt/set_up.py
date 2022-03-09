import keyring
import os
from pathlib import Path
from configparser import ConfigParser
import sqlalchemy
import logging
import base64

_logger = logging.getLogger(__name__)
_CONFIG_PTH = Path.home() / ".dwopt"
_KEYRING_SERV_ID = Path(__file__).parent.resolve().as_posix()


def save_url(db_nme, url, method="keyring"):
    """Save encoded database engine url to keyring or config.

    The package can also read a manually set environment variable storing the raw url.
    See examples for quick-start.

    On package import, default url are taken firstly from keyring if available,
    then the config file if available, then the environment variable
    if available, lastly a set of hard-coded dummy urls.

    A `sqlalchemy engine url <https://docs.sqlalchemy.org/en/14/core/
    engines.html#database-urls>`_
    combines the user name, password, database names, etc
    into a single string.

    *Details on credential locations:*

    * The system keyring service is accessed via the
      `keyring <https://pypi.org/project/keyring/>`_
      package. The service on Windows is the Windows Credential Manager.
      The service id is the full path to the dwopt package files.
      For example: ``E:\\python\\python3.9\\Lib\\site-packages\\dwopt``.
      The user name will be either ``pg``, ``lt``, or ``oc``.
      The url will be encoded before saving.

    * The config file is created with name ``.dwopt``
      on the system ``HOME`` directory.
      There will be a url section on the config file, with option names being
      the database names. The url will be encoded before saving.

    * The environment variables should be manually made with the names
      ``dwopt_pg``, ``dwopt_lt`` or ``dwopt_oc``, and the value being
      the raw url string.

    Base 64 encoding is done to prevent raw password being stored on files.
    User could rewrite the ``_encode`` and the ``_decode`` functions to implement
    custom encryption algorithm.

    Parameters
    ----------
    db_nme : str
        Relevant database code. Either ``pg`` for postgre, ``lt`` for sqlite,
        or ``oc`` for oracle.
    url : str
        Sqlalchemy engine url.
    method: str
        Method used to save, either 'keyring', or 'config'.
        Default 'keyring'.

    Returns
    -------
    str
        Message anouncing completion.

    Examples
    --------
    Save connection urls in various methods for various databases.
    Also manually create following environment variable
    ``variable = value`` pair: ``dwopt_lt = sqlite:///:memory:``.

    >>> import dwopt
    >>> dwopt.save_url('pg', 'postgresql://scott2:tiger@localhost/mydatabase')
        'Saved pg url to keyring'
    >>> dwopt.save_url('oc', 'oracle://scott2:tiger@tnsname', 'config')
        'Saved oc url to config'

    Exit and re-enter python for changes to take effect.

    >>> from dwopt import pg, lt, oc
    >>> pg.run('select * from test')
    >>> lt.list_tables()
    >>> oc.qry('dual').head()
    """
    url = _encode(url)
    if method == "keyring":
        keyring.set_password(_KEYRING_SERV_ID, db_nme, url)
    elif method == "config":
        cfg = ConfigParser()
        cfg.read(_CONFIG_PTH)
        if not cfg.has_section("url"):
            cfg.add_section("url")
        cfg.set("url", db_nme, url)
        with open(_CONFIG_PTH, "w") as f:
            cfg.write(f)
    else:
        raise ValueError("Invalid method, either 'keyring', or 'config'")
    return f"Saved {db_nme} url to {method}"


def _get_url(db_nme):
    """Get url if available, else dummy url."""
    url = None

    try:
        url = _decode(keyring.get_password(_KEYRING_SERV_ID, db_nme))
    except Exception as e:
        _logger.warning(e)
    if url is not None:
        _logger.debug(f"{db_nme} url obtained from keyring")
        return url

    cfg = ConfigParser()
    cfg.read(_CONFIG_PTH)
    if cfg.has_option("url", db_nme):
        url = _decode(cfg.get("url", db_nme))
    if url is not None:
        _logger.debug(f"{db_nme} url obtained from config")
        return url

    url = os.environ.get(f"dwopt_{db_nme}")
    if url is not None:
        _logger.debug(f"{db_nme} url obtained from environ")
        return url

    if db_nme == "pg":
        url = "postgresql://scott:tiger@localhost/mydatabase"
    elif db_nme == "lt":
        url = "sqlite://"
    elif db_nme == "oc":
        url = "oracle://scott:tiger@tnsname"
    else:
        raise ValueError("Invalid db_nme, either 'pg', 'lt' or 'oc'")
    _logger.debug(f"{db_nme} url obtained from hardcoded dummy")
    return url


def _encode(x):
    if x is not None:
        return base64.b64encode(x.encode("UTF-8")).decode("UTF-8")


def _decode(x):
    if x is not None:
        return base64.b64decode(x.encode("UTF-8")).decode("UTF-8")


def make_eng(url):
    """Make database connection engine.

    Use the database connection engine to instantiate the database opeartor class.
    This function is provided outside of the class because the engine object is
    best to be created only once per application.

    A `sqlalchemy engine url <https://docs.sqlalchemy.org/en/14/
    core/engines.html#database-urls>`_
    combines the user name, password, database names, etc
    into a single string.

    Parameters
    ----------
    url : str
        Sqlalchemy engine url.

    Returns
    -------
    sqlalchemy engine

    Examples
    --------
    Instantiate and use a database operator object.

    >>> from dwopt import make_eng, Pg
    >>> url = "postgresql://dwopt_tester:1234@localhost/dwopt_test"
    >>> pg_eng = make_eng(url)
    >>> pg = Pg(pg_eng)
    >>> pg.run('select count(1) from test')
        42
    """
    return sqlalchemy.create_engine(url)


def make_meta(eng, schema):
    pass
