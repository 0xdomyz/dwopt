import base64
import json
import logging
import os
from configparser import ConfigParser
from pathlib import Path
from typing import Tuple, Union

import keyring
import pandas as pd
import sqlalchemy

_logger = logging.getLogger(__name__)
_CONFIG_PTH = Path.home() / ".dwopt"
_KEYRING_SERV_ID = Path(__file__).parent.resolve().as_posix()
_TEST_PG_URL = "postgresql://dwopt_tester:1234@localhost/dwopt_test"
_TEST_LT_URL = "sqlite://"
_TEST_OC_URL = (
    "oracle+oracledb://dwopt_test:1234@localhost:1521/?service_name=XEPDB1"
    "&encoding=UTF-8&nencoding=UTF-8"
)


def save_url(db_nme: str, url: str = None, method: str = "keyring", **kwargs):
    """Save or delete encoded database engine url to keyring or config.

    See notes for details, see examples for quick-start.

    A `sqlalchemy engine url <https://docs.sqlalchemy.org/en/14/core/
    engines.html#database-urls>`_
    combines the user name, password, database names, etc
    into a single string.

    Parameters
    ----------
    db_nme : str
        Relevant database code. Either ``pg`` for postgre, ``lt`` for sqlite,
        or ``oc`` for oracle.
    url : str or None
        Sqlalchemy engine url.
        None to delete url from keyring or config.
    method: str
        Method used to save, either 'keyring', or 'config'.
        Default 'keyring'.
    kwargs: Additional engine creation parameters.

    Returns
    -------
    str
        Message anouncing completion.

    Notes
    ------
    Credential locations:

    * The system keyring service is accessed via the
      `keyring <https://pypi.org/project/keyring/>`_
      package. The service on Windows is the Windows Credential Manager.
      The service id is the full path to the dwopt package files.
      For example: ``E:\\python\\python3.9\\Lib\\site-packages\\dwopt``.
      The user name will be either ``pg``, ``lt``, or ``oc``.
      The url will be encoded before saving.

      Does not work if no keyring service is not available on
      the system, see keyring package documentation for details.

    * The config file is created with name ``.dwopt``
      on the system ``HOME`` directory.
      There will be a url section on the config file, with option names being
      the database names. The url will be encoded before saving.

    * The environment variables should be manually made with the names
      ``dwopt_pg``, ``dwopt_lt`` or ``dwopt_oc``, and the value being
      the raw url string, or string representing dictionary of engine parameters,
      see examples.

    Base 64 encoding is done to prevent raw password being stored on files.
    User could rewrite the ``_encode`` and the ``_decode`` functions to implement
    custom encryption algorithm.

    On package import, default url are taken firstly from keyring if available,
    then the config file if available, then the environment variable
    if available, lastly a set of hard-coded testing urls.

    To extend this feature and add additional default urls, add to the instantiation
    lines in the ``__init__.py`` file, and save url to the symbol, example::

        dev = db(_get_url("lt_dev")) # add to __init__.py
        dwopt.save_url('lt_dev', "sqlite://")# run in a session

    Examples
    --------
    Save connection urls in various methods for various databases.
    Also manually create following environment variable
    ``variable = value`` pair: ``dwopt_lt = sqlite://``.

    >>> import dwopt
    >>> dwopt.save_url('pg', 'postgresql://dwopt_tester:1234@localhost/dwopt_test')
    'Saved pg url to keyring'
    >>> url = ("oracle+oracledb://dwopt_test:1234@localhost:1521/?service_name=XEPDB1"
    ...     "&encoding=UTF-8&nencoding=UTF-8")
    >>>
    >>> dwopt.save_url('oc', url, 'config')
    'Saved oc url to config'

    Exit and re-enter python for changes to take effect.

    >>> from dwopt import pg, lt, oc
    >>> pg.eng
    Engine(postgresql://dwopt_tester:***@localhost/dwopt_test)
    >>> lt.eng
    Engine(sqlite://)
    >>> str(oc.eng)[:50]
    'Engine(oracle+oracledb://dwopt_test:***@localhost:'

    Remove saved urls.

    >>> dwopt.save_url('pg', url=None, method='keyring')
    'Deleted pg url from keyring'
    >>> dwopt.save_url('oc', method='config')
    'Deleted oc url from config'

    Save additional engine parameters alongside url,
    username for location of oracle bin to be replaced by actual username::

        import dwopt
        dwopt.save_url(
            db_nme='oc',
            url=(
                "oracle+oracledb://dwopt_test:1234@localhost:1521/"
                "?service_name=XEPDB1 &encoding=UTF-8&nencoding=UTF-8"
            ),
            method='keyring',
            thick_mode={"lib_dir":"C:/app/{username}/product/21c/dbhomeXE/bin"}
        )
        # restart python
        from dwopt import oc
        oc.run("select * from dual")

        # check if thick mode is enabled
        import oracledb
        oracledb.is_thin_mode()# False

    Environment variable storing additional parameters,
    example ``variable = value`` pair:
    ``dwopt_oc = {"url": "oracle+oracledb://dwopt_test:1234@localhost:1521/
    ?service_name=XEPDB1 &encoding=UTF-8&nencoding=UTF-8",
    "thick_mode":{"lib_dir":"C:/app/{user_name}/product/21c/dbhomeXE/bin"}}``
    . Then assuming oc url not saved to keyring or config::

        from dwopt import oc
        oc.run("select * from dual")

    """
    # create url with possible additional parameters
    if url is not None:
        kwargs.update({"url": url})
        dict_url = json.dumps(kwargs)
        encoded_url = _encode(dict_url)
    else:
        encoded_url = None

    # save url
    if method == "keyring":
        if encoded_url is None:
            keyring.delete_password(_KEYRING_SERV_ID, db_nme)
        else:
            keyring.set_password(_KEYRING_SERV_ID, db_nme, encoded_url)
    elif method == "config":
        cfg = ConfigParser()
        cfg.read(_CONFIG_PTH)
        if not cfg.has_section("url"):
            cfg.add_section("url")

        if encoded_url is None:
            cfg.remove_option("url", db_nme)
        else:
            cfg.set("url", db_nme, encoded_url)

        with open(_CONFIG_PTH, "w") as f:
            cfg.write(f)
    else:
        raise ValueError("Invalid method, either 'keyring', or 'config'")

    if encoded_url is None:
        _logger.debug(f"{db_nme} url deleted from {method}")
        return f"Deleted {db_nme} url from {method}"
    else:
        _logger.debug(f"{db_nme} url saved to {method}")
        return f"Saved {db_nme} url to {method}"


def _parse_saved_url(raw: str) -> Tuple[str, dict]:
    """
    Examples
    --------
    >>> _parse_saved_url('sqlite://')
    ('sqlite://', {})
    >>> _parse_saved_url('{"url": "sqlite://"}')
    ('sqlite://', {})
    >>> _parse_saved_url('{"url": "sqlite://", "echo": true}')
    ('sqlite://', {'echo': True})
    """
    if '"url":' in raw:
        try:
            res = json.loads(raw)
        except Exception as e:
            raise ValueError(f"invalid json format: {raw}") from e
        if isinstance(res, dict):
            url = res["url"]
            res.pop("url")
            kwargs = res
        elif isinstance(res, str):
            url = res
            kwargs = {}
        else:
            raise ValueError(f"unknown loaded url format: {res}")
    else:  # backward compatibility
        url = raw
        kwargs = {}

    return url, kwargs


def _get_url(db_nme: str) -> Tuple[str, dict]:
    """Get url if available, else dummy url."""
    url = None

    # keyring
    try:
        url = keyring.get_password(_KEYRING_SERV_ID, db_nme)
        if url is not None:
            url = _decode(url)
    except Exception as e:
        _logger.warning(e)
    if url is not None:
        _logger.debug(f"{db_nme} url obtained from keyring")
        return _parse_saved_url(raw=url)

    # config
    cfg = ConfigParser()
    cfg.read(_CONFIG_PTH)
    if cfg.has_option("url", db_nme):
        url = cfg.get("url", db_nme)
        if url is not None:
            url = _decode(url)
    if url is not None:
        _logger.debug(f"{db_nme} url obtained from config")
        return _parse_saved_url(raw=url)

    # environ
    url = os.environ.get(f"dwopt_{db_nme}")
    if url is not None:
        _logger.debug(f"{db_nme} url obtained from environ")
        return _parse_saved_url(raw=url)

    # dummy
    if db_nme == "pg":
        url = _TEST_PG_URL
    elif db_nme == "lt":
        url = _TEST_LT_URL
    elif db_nme == "oc":
        url = _TEST_OC_URL
    else:
        raise ValueError("Invalid db_nme, either 'pg', 'lt' or 'oc'")
    _logger.debug(f"{db_nme} url obtained from hardcoded testing url")
    return _parse_saved_url(raw=url)


# todo remove none case
def _encode(x: str) -> str:
    return base64.b64encode(x.encode("UTF-8")).decode("UTF-8")


def _decode(x: str) -> str:
    return base64.b64decode(x.encode("UTF-8")).decode("UTF-8")


def make_eng(url, **kwargs):
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
    kwargs : additional engine creation parameters.

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
    >>> pg.iris()
    >>> pg.run('select count(1) from iris')
       count
    0    150
    """
    return sqlalchemy.create_engine(url, **kwargs)


def make_meta(eng, schema):
    pass


def _make_iris_df():
    res = [
        {"v1": 5.1, "v2": 3.5, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.9, "v2": 3.0, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.7, "v2": 3.2, "v3": 1.3, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.6, "v2": 3.1, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.6, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.4, "v2": 3.9, "v3": 1.7, "v4": 0.4, "v5": "setosa"},
        {"v1": 4.6, "v2": 3.4, "v3": 1.4, "v4": 0.3, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.4, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.4, "v2": 2.9, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.9, "v2": 3.1, "v3": 1.5, "v4": 0.1, "v5": "setosa"},
        {"v1": 5.4, "v2": 3.7, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.8, "v2": 3.4, "v3": 1.6, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.8, "v2": 3.0, "v3": 1.4, "v4": 0.1, "v5": "setosa"},
        {"v1": 4.3, "v2": 3.0, "v3": 1.1, "v4": 0.1, "v5": "setosa"},
        {"v1": 5.8, "v2": 4.0, "v3": 1.2, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.7, "v2": 4.4, "v3": 1.5, "v4": 0.4, "v5": "setosa"},
        {"v1": 5.4, "v2": 3.9, "v3": 1.3, "v4": 0.4, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.5, "v3": 1.4, "v4": 0.3, "v5": "setosa"},
        {"v1": 5.7, "v2": 3.8, "v3": 1.7, "v4": 0.3, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.8, "v3": 1.5, "v4": 0.3, "v5": "setosa"},
        {"v1": 5.4, "v2": 3.4, "v3": 1.7, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.7, "v3": 1.5, "v4": 0.4, "v5": "setosa"},
        {"v1": 4.6, "v2": 3.6, "v3": 1.0, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.3, "v3": 1.7, "v4": 0.5, "v5": "setosa"},
        {"v1": 4.8, "v2": 3.4, "v3": 1.9, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.0, "v3": 1.6, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.4, "v3": 1.6, "v4": 0.4, "v5": "setosa"},
        {"v1": 5.2, "v2": 3.5, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.2, "v2": 3.4, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.7, "v2": 3.2, "v3": 1.6, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.8, "v2": 3.1, "v3": 1.6, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.4, "v2": 3.4, "v3": 1.5, "v4": 0.4, "v5": "setosa"},
        {"v1": 5.2, "v2": 4.1, "v3": 1.5, "v4": 0.1, "v5": "setosa"},
        {"v1": 5.5, "v2": 4.2, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.9, "v2": 3.1, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.2, "v3": 1.2, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.5, "v2": 3.5, "v3": 1.3, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.9, "v2": 3.6, "v3": 1.4, "v4": 0.1, "v5": "setosa"},
        {"v1": 4.4, "v2": 3.0, "v3": 1.3, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.4, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.5, "v3": 1.3, "v4": 0.3, "v5": "setosa"},
        {"v1": 4.5, "v2": 2.3, "v3": 1.3, "v4": 0.3, "v5": "setosa"},
        {"v1": 4.4, "v2": 3.2, "v3": 1.3, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.5, "v3": 1.6, "v4": 0.6, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.8, "v3": 1.9, "v4": 0.4, "v5": "setosa"},
        {"v1": 4.8, "v2": 3.0, "v3": 1.4, "v4": 0.3, "v5": "setosa"},
        {"v1": 5.1, "v2": 3.8, "v3": 1.6, "v4": 0.2, "v5": "setosa"},
        {"v1": 4.6, "v2": 3.2, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.3, "v2": 3.7, "v3": 1.5, "v4": 0.2, "v5": "setosa"},
        {"v1": 5.0, "v2": 3.3, "v3": 1.4, "v4": 0.2, "v5": "setosa"},
        {"v1": 7.0, "v2": 3.2, "v3": 4.7, "v4": 1.4, "v5": "sicolor"},
        {"v1": 6.4, "v2": 3.2, "v3": 4.5, "v4": 1.5, "v5": "sicolor"},
        {"v1": 6.9, "v2": 3.1, "v3": 4.9, "v4": 1.5, "v5": "sicolor"},
        {"v1": 5.5, "v2": 2.3, "v3": 4.0, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.5, "v2": 2.8, "v3": 4.6, "v4": 1.5, "v5": "sicolor"},
        {"v1": 5.7, "v2": 2.8, "v3": 4.5, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.3, "v2": 3.3, "v3": 4.7, "v4": 1.6, "v5": "sicolor"},
        {"v1": 4.9, "v2": 2.4, "v3": 3.3, "v4": 1.0, "v5": "sicolor"},
        {"v1": 6.6, "v2": 2.9, "v3": 4.6, "v4": 1.3, "v5": "sicolor"},
        {"v1": 5.2, "v2": 2.7, "v3": 3.9, "v4": 1.4, "v5": "sicolor"},
        {"v1": 5.0, "v2": 2.0, "v3": 3.5, "v4": 1.0, "v5": "sicolor"},
        {"v1": 5.9, "v2": 3.0, "v3": 4.2, "v4": 1.5, "v5": "sicolor"},
        {"v1": 6.0, "v2": 2.2, "v3": 4.0, "v4": 1.0, "v5": "sicolor"},
        {"v1": 6.1, "v2": 2.9, "v3": 4.7, "v4": 1.4, "v5": "sicolor"},
        {"v1": 5.6, "v2": 2.9, "v3": 3.6, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.7, "v2": 3.1, "v3": 4.4, "v4": 1.4, "v5": "sicolor"},
        {"v1": 5.6, "v2": 3.0, "v3": 4.5, "v4": 1.5, "v5": "sicolor"},
        {"v1": 5.8, "v2": 2.7, "v3": 4.1, "v4": 1.0, "v5": "sicolor"},
        {"v1": 6.2, "v2": 2.2, "v3": 4.5, "v4": 1.5, "v5": "sicolor"},
        {"v1": 5.6, "v2": 2.5, "v3": 3.9, "v4": 1.1, "v5": "sicolor"},
        {"v1": 5.9, "v2": 3.2, "v3": 4.8, "v4": 1.8, "v5": "sicolor"},
        {"v1": 6.1, "v2": 2.8, "v3": 4.0, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.3, "v2": 2.5, "v3": 4.9, "v4": 1.5, "v5": "sicolor"},
        {"v1": 6.1, "v2": 2.8, "v3": 4.7, "v4": 1.2, "v5": "sicolor"},
        {"v1": 6.4, "v2": 2.9, "v3": 4.3, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.6, "v2": 3.0, "v3": 4.4, "v4": 1.4, "v5": "sicolor"},
        {"v1": 6.8, "v2": 2.8, "v3": 4.8, "v4": 1.4, "v5": "sicolor"},
        {"v1": 6.7, "v2": 3.0, "v3": 5.0, "v4": 1.7, "v5": "sicolor"},
        {"v1": 6.0, "v2": 2.9, "v3": 4.5, "v4": 1.5, "v5": "sicolor"},
        {"v1": 5.7, "v2": 2.6, "v3": 3.5, "v4": 1.0, "v5": "sicolor"},
        {"v1": 5.5, "v2": 2.4, "v3": 3.8, "v4": 1.1, "v5": "sicolor"},
        {"v1": 5.5, "v2": 2.4, "v3": 3.7, "v4": 1.0, "v5": "sicolor"},
        {"v1": 5.8, "v2": 2.7, "v3": 3.9, "v4": 1.2, "v5": "sicolor"},
        {"v1": 6.0, "v2": 2.7, "v3": 5.1, "v4": 1.6, "v5": "sicolor"},
        {"v1": 5.4, "v2": 3.0, "v3": 4.5, "v4": 1.5, "v5": "sicolor"},
        {"v1": 6.0, "v2": 3.4, "v3": 4.5, "v4": 1.6, "v5": "sicolor"},
        {"v1": 6.7, "v2": 3.1, "v3": 4.7, "v4": 1.5, "v5": "sicolor"},
        {"v1": 6.3, "v2": 2.3, "v3": 4.4, "v4": 1.3, "v5": "sicolor"},
        {"v1": 5.6, "v2": 3.0, "v3": 4.1, "v4": 1.3, "v5": "sicolor"},
        {"v1": 5.5, "v2": 2.5, "v3": 4.0, "v4": 1.3, "v5": "sicolor"},
        {"v1": 5.5, "v2": 2.6, "v3": 4.4, "v4": 1.2, "v5": "sicolor"},
        {"v1": 6.1, "v2": 3.0, "v3": 4.6, "v4": 1.4, "v5": "sicolor"},
        {"v1": 5.8, "v2": 2.6, "v3": 4.0, "v4": 1.2, "v5": "sicolor"},
        {"v1": 5.0, "v2": 2.3, "v3": 3.3, "v4": 1.0, "v5": "sicolor"},
        {"v1": 5.6, "v2": 2.7, "v3": 4.2, "v4": 1.3, "v5": "sicolor"},
        {"v1": 5.7, "v2": 3.0, "v3": 4.2, "v4": 1.2, "v5": "sicolor"},
        {"v1": 5.7, "v2": 2.9, "v3": 4.2, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.2, "v2": 2.9, "v3": 4.3, "v4": 1.3, "v5": "sicolor"},
        {"v1": 5.1, "v2": 2.5, "v3": 3.0, "v4": 1.1, "v5": "sicolor"},
        {"v1": 5.7, "v2": 2.8, "v3": 4.1, "v4": 1.3, "v5": "sicolor"},
        {"v1": 6.3, "v2": 3.3, "v3": 6.0, "v4": 2.5, "v5": "rginica"},
        {"v1": 5.8, "v2": 2.7, "v3": 5.1, "v4": 1.9, "v5": "rginica"},
        {"v1": 7.1, "v2": 3.0, "v3": 5.9, "v4": 2.1, "v5": "rginica"},
        {"v1": 6.3, "v2": 2.9, "v3": 5.6, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.5, "v2": 3.0, "v3": 5.8, "v4": 2.2, "v5": "rginica"},
        {"v1": 7.6, "v2": 3.0, "v3": 6.6, "v4": 2.1, "v5": "rginica"},
        {"v1": 4.9, "v2": 2.5, "v3": 4.5, "v4": 1.7, "v5": "rginica"},
        {"v1": 7.3, "v2": 2.9, "v3": 6.3, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.7, "v2": 2.5, "v3": 5.8, "v4": 1.8, "v5": "rginica"},
        {"v1": 7.2, "v2": 3.6, "v3": 6.1, "v4": 2.5, "v5": "rginica"},
        {"v1": 6.5, "v2": 3.2, "v3": 5.1, "v4": 2.0, "v5": "rginica"},
        {"v1": 6.4, "v2": 2.7, "v3": 5.3, "v4": 1.9, "v5": "rginica"},
        {"v1": 6.8, "v2": 3.0, "v3": 5.5, "v4": 2.1, "v5": "rginica"},
        {"v1": 5.7, "v2": 2.5, "v3": 5.0, "v4": 2.0, "v5": "rginica"},
        {"v1": 5.8, "v2": 2.8, "v3": 5.1, "v4": 2.4, "v5": "rginica"},
        {"v1": 6.4, "v2": 3.2, "v3": 5.3, "v4": 2.3, "v5": "rginica"},
        {"v1": 6.5, "v2": 3.0, "v3": 5.5, "v4": 1.8, "v5": "rginica"},
        {"v1": 7.7, "v2": 3.8, "v3": 6.7, "v4": 2.2, "v5": "rginica"},
        {"v1": 7.7, "v2": 2.6, "v3": 6.9, "v4": 2.3, "v5": "rginica"},
        {"v1": 6.0, "v2": 2.2, "v3": 5.0, "v4": 1.5, "v5": "rginica"},
        {"v1": 6.9, "v2": 3.2, "v3": 5.7, "v4": 2.3, "v5": "rginica"},
        {"v1": 5.6, "v2": 2.8, "v3": 4.9, "v4": 2.0, "v5": "rginica"},
        {"v1": 7.7, "v2": 2.8, "v3": 6.7, "v4": 2.0, "v5": "rginica"},
        {"v1": 6.3, "v2": 2.7, "v3": 4.9, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.7, "v2": 3.3, "v3": 5.7, "v4": 2.1, "v5": "rginica"},
        {"v1": 7.2, "v2": 3.2, "v3": 6.0, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.2, "v2": 2.8, "v3": 4.8, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.1, "v2": 3.0, "v3": 4.9, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.4, "v2": 2.8, "v3": 5.6, "v4": 2.1, "v5": "rginica"},
        {"v1": 7.2, "v2": 3.0, "v3": 5.8, "v4": 1.6, "v5": "rginica"},
        {"v1": 7.4, "v2": 2.8, "v3": 6.1, "v4": 1.9, "v5": "rginica"},
        {"v1": 7.9, "v2": 3.8, "v3": 6.4, "v4": 2.0, "v5": "rginica"},
        {"v1": 6.4, "v2": 2.8, "v3": 5.6, "v4": 2.2, "v5": "rginica"},
        {"v1": 6.3, "v2": 2.8, "v3": 5.1, "v4": 1.5, "v5": "rginica"},
        {"v1": 6.1, "v2": 2.6, "v3": 5.6, "v4": 1.4, "v5": "rginica"},
        {"v1": 7.7, "v2": 3.0, "v3": 6.1, "v4": 2.3, "v5": "rginica"},
        {"v1": 6.3, "v2": 3.4, "v3": 5.6, "v4": 2.4, "v5": "rginica"},
        {"v1": 6.4, "v2": 3.1, "v3": 5.5, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.0, "v2": 3.0, "v3": 4.8, "v4": 1.8, "v5": "rginica"},
        {"v1": 6.9, "v2": 3.1, "v3": 5.4, "v4": 2.1, "v5": "rginica"},
        {"v1": 6.7, "v2": 3.1, "v3": 5.6, "v4": 2.4, "v5": "rginica"},
        {"v1": 6.9, "v2": 3.1, "v3": 5.1, "v4": 2.3, "v5": "rginica"},
        {"v1": 5.8, "v2": 2.7, "v3": 5.1, "v4": 1.9, "v5": "rginica"},
        {"v1": 6.8, "v2": 3.2, "v3": 5.9, "v4": 2.3, "v5": "rginica"},
        {"v1": 6.7, "v2": 3.3, "v3": 5.7, "v4": 2.5, "v5": "rginica"},
        {"v1": 6.7, "v2": 3.0, "v3": 5.2, "v4": 2.3, "v5": "rginica"},
        {"v1": 6.3, "v2": 2.5, "v3": 5.0, "v4": 1.9, "v5": "rginica"},
        {"v1": 6.5, "v2": 3.0, "v3": 5.2, "v4": 2.0, "v5": "rginica"},
        {"v1": 6.2, "v2": 3.4, "v3": 5.4, "v4": 2.3, "v5": "rginica"},
        {"v1": 5.9, "v2": 3.0, "v3": 5.1, "v4": 1.8, "v5": "rginica"},
    ]
    res = pd.DataFrame(res)
    res.columns = [
        "Sepal.Length",
        "Sepal.Width",
        "Petal.Length",
        "Petal.Width",
        "Species",
    ]
    return res


def _make_mtcars_df():
    res1 = [
        {"v0": "Mazda RX4"},
        {"v0": "Mazda RX4 Wag"},
        {"v0": "Datsun 710"},
        {"v0": "Hornet 4 Drive"},
        {"v0": "Hornet Sportabout"},
        {"v0": "Valiant"},
        {"v0": "Duster 360"},
        {"v0": "Merc 240D"},
        {"v0": "Merc 230"},
        {"v0": "Merc 280"},
        {"v0": "Merc 280C"},
        {"v0": "Merc 450SE"},
        {"v0": "Merc 450SL"},
        {"v0": "Merc 450SLC"},
        {"v0": "Cadillac Fleetwood"},
        {"v0": "Lincoln Continental"},
        {"v0": "Chrysler Imperial"},
        {"v0": "Fiat 128"},
        {"v0": "Honda Civic"},
        {"v0": "Toyota Corolla"},
        {"v0": "Toyota Corona"},
        {"v0": "Dodge Challenger"},
        {"v0": "AMC Javelin"},
        {"v0": "Camaro Z28"},
        {"v0": "Pontiac Firebird"},
        {"v0": "Fiat X1-9"},
        {"v0": "Porsche 914-2"},
        {"v0": "Lotus Europa"},
        {"v0": "Ford Pantera L"},
        {"v0": "Ferrari Dino"},
        {"v0": "Maserati Bora"},
        {"v0": "Volvo 142E"},
    ]
    res2 = [
        {"v1": 21.0, "v2": 6, "v3": 160.0, "v4": 110},
        {"v1": 21.0, "v2": 6, "v3": 160.0, "v4": 110},
        {"v1": 22.8, "v2": 4, "v3": 108.0, "v4": 93},
        {"v1": 21.4, "v2": 6, "v3": 258.0, "v4": 110},
        {"v1": 18.7, "v2": 8, "v3": 360.0, "v4": 175},
        {"v1": 18.1, "v2": 6, "v3": 225.0, "v4": 105},
        {"v1": 14.3, "v2": 8, "v3": 360.0, "v4": 245},
        {"v1": 24.4, "v2": 4, "v3": 146.7, "v4": 62},
        {"v1": 22.8, "v2": 4, "v3": 140.8, "v4": 95},
        {"v1": 19.2, "v2": 6, "v3": 167.6, "v4": 123},
        {"v1": 17.8, "v2": 6, "v3": 167.6, "v4": 123},
        {"v1": 16.4, "v2": 8, "v3": 275.8, "v4": 180},
        {"v1": 17.3, "v2": 8, "v3": 275.8, "v4": 180},
        {"v1": 15.2, "v2": 8, "v3": 275.8, "v4": 180},
        {"v1": 10.4, "v2": 8, "v3": 472.0, "v4": 205},
        {"v1": 10.4, "v2": 8, "v3": 460.0, "v4": 215},
        {"v1": 14.7, "v2": 8, "v3": 440.0, "v4": 230},
        {"v1": 32.4, "v2": 4, "v3": 78.7, "v4": 66},
        {"v1": 30.4, "v2": 4, "v3": 75.7, "v4": 52},
        {"v1": 33.9, "v2": 4, "v3": 71.1, "v4": 65},
        {"v1": 21.5, "v2": 4, "v3": 120.1, "v4": 97},
        {"v1": 15.5, "v2": 8, "v3": 318.0, "v4": 150},
        {"v1": 15.2, "v2": 8, "v3": 304.0, "v4": 150},
        {"v1": 13.3, "v2": 8, "v3": 350.0, "v4": 245},
        {"v1": 19.2, "v2": 8, "v3": 400.0, "v4": 175},
        {"v1": 27.3, "v2": 4, "v3": 79.0, "v4": 66},
        {"v1": 26.0, "v2": 4, "v3": 120.3, "v4": 91},
        {"v1": 30.4, "v2": 4, "v3": 95.1, "v4": 113},
        {"v1": 15.8, "v2": 8, "v3": 351.0, "v4": 264},
        {"v1": 19.7, "v2": 6, "v3": 145.0, "v4": 175},
        {"v1": 15.0, "v2": 8, "v3": 301.0, "v4": 335},
        {"v1": 21.4, "v2": 4, "v3": 121.0, "v4": 109},
    ]
    res3 = [
        {"v5": 3.90, "v6": 2.620, "v7": 16.46, "v8": 0, "v9": 1, "w0": 4, "w1": 4},
        {"v5": 3.90, "v6": 2.875, "v7": 17.02, "v8": 0, "v9": 1, "w0": 4, "w1": 4},
        {"v5": 3.85, "v6": 2.320, "v7": 18.61, "v8": 1, "v9": 1, "w0": 4, "w1": 1},
        {"v5": 3.08, "v6": 3.215, "v7": 19.44, "v8": 1, "v9": 0, "w0": 3, "w1": 1},
        {"v5": 3.15, "v6": 3.440, "v7": 17.02, "v8": 0, "v9": 0, "w0": 3, "w1": 2},
        {"v5": 2.76, "v6": 3.460, "v7": 20.22, "v8": 1, "v9": 0, "w0": 3, "w1": 1},
        {"v5": 3.21, "v6": 3.570, "v7": 15.84, "v8": 0, "v9": 0, "w0": 3, "w1": 4},
        {"v5": 3.69, "v6": 3.190, "v7": 20.00, "v8": 1, "v9": 0, "w0": 4, "w1": 2},
        {"v5": 3.92, "v6": 3.150, "v7": 22.90, "v8": 1, "v9": 0, "w0": 4, "w1": 2},
        {"v5": 3.92, "v6": 3.440, "v7": 18.30, "v8": 1, "v9": 0, "w0": 4, "w1": 4},
        {"v5": 3.92, "v6": 3.440, "v7": 18.90, "v8": 1, "v9": 0, "w0": 4, "w1": 4},
        {"v5": 3.07, "v6": 4.070, "v7": 17.40, "v8": 0, "v9": 0, "w0": 3, "w1": 3},
        {"v5": 3.07, "v6": 3.730, "v7": 17.60, "v8": 0, "v9": 0, "w0": 3, "w1": 3},
        {"v5": 3.07, "v6": 3.780, "v7": 18.00, "v8": 0, "v9": 0, "w0": 3, "w1": 3},
        {"v5": 2.93, "v6": 5.250, "v7": 17.98, "v8": 0, "v9": 0, "w0": 3, "w1": 4},
        {"v5": 3.00, "v6": 5.424, "v7": 17.82, "v8": 0, "v9": 0, "w0": 3, "w1": 4},
        {"v5": 3.23, "v6": 5.345, "v7": 17.42, "v8": 0, "v9": 0, "w0": 3, "w1": 4},
        {"v5": 4.08, "v6": 2.200, "v7": 19.47, "v8": 1, "v9": 1, "w0": 4, "w1": 1},
        {"v5": 4.93, "v6": 1.615, "v7": 18.52, "v8": 1, "v9": 1, "w0": 4, "w1": 2},
        {"v5": 4.22, "v6": 1.835, "v7": 19.90, "v8": 1, "v9": 1, "w0": 4, "w1": 1},
        {"v5": 3.70, "v6": 2.465, "v7": 20.01, "v8": 1, "v9": 0, "w0": 3, "w1": 1},
        {"v5": 2.76, "v6": 3.520, "v7": 16.87, "v8": 0, "v9": 0, "w0": 3, "w1": 2},
        {"v5": 3.15, "v6": 3.435, "v7": 17.30, "v8": 0, "v9": 0, "w0": 3, "w1": 2},
        {"v5": 3.73, "v6": 3.840, "v7": 15.41, "v8": 0, "v9": 0, "w0": 3, "w1": 4},
        {"v5": 3.08, "v6": 3.845, "v7": 17.05, "v8": 0, "v9": 0, "w0": 3, "w1": 2},
        {"v5": 4.08, "v6": 1.935, "v7": 18.90, "v8": 1, "v9": 1, "w0": 4, "w1": 1},
        {"v5": 4.43, "v6": 2.140, "v7": 16.70, "v8": 0, "v9": 1, "w0": 5, "w1": 2},
        {"v5": 3.77, "v6": 1.513, "v7": 16.90, "v8": 1, "v9": 1, "w0": 5, "w1": 2},
        {"v5": 4.22, "v6": 3.170, "v7": 14.50, "v8": 0, "v9": 1, "w0": 5, "w1": 4},
        {"v5": 3.62, "v6": 2.770, "v7": 15.50, "v8": 0, "v9": 1, "w0": 5, "w1": 6},
        {"v5": 3.54, "v6": 3.570, "v7": 14.60, "v8": 0, "v9": 1, "w0": 5, "w1": 8},
        {"v5": 4.11, "v6": 2.780, "v7": 18.60, "v8": 1, "v9": 1, "w0": 4, "w1": 2},
    ]

    res = pd.DataFrame([dict(**i, **j, **k) for i, j, k in zip(res1, res2, res3)])
    res.columns = [
        "name",
        "mpg",
        "cyl",
        "disp",
        "hp",
        "drat",
        "wt",
        "qsec",
        "vs",
        "am",
        "gear",
        "carb",
    ]
    return res
