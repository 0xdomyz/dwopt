import logging

from .dbo import Db, Lt, Oc, Pg, db
from .set_up import _get_url, make_eng, save_url
from .testing import make_test_tbl

_logger = logging.getLogger(__name__)

try:
    url, kwargs = _get_url("lt")
    lt = db(url, **kwargs)
except Exception as e:
    msg = f"Failed to initialize default lt operator: {e}"
    _logger.debug(msg)
    lt = msg

try:
    url, kwargs = _get_url("pg")
    pg = db(url, **kwargs)
except Exception as e:
    msg = f"Failed to initialize default lt operator: {e}"
    _logger.debug(msg)
    pg = msg

try:
    url, kwargs = _get_url("oc")
    oc = db(url, **kwargs)
except Exception as e:
    msg = f"Failed to initialize default lt operator: {e}"
    _logger.debug(msg)
    oc = msg
