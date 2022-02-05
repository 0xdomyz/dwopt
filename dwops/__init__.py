from .db import Pg, Lt, Oc
from .urls import (
    _make_wallet_if_not_exist
    ,_get_default_url
    ,save_default_url
    ,make_eng
)

_make_wallet_if_not_exist()

pg = Pg(make_eng(_get_default_url('postgre')))
lt = Lt(make_eng(_get_default_url('sqlite')))
oc = Oc(make_eng(_get_default_url('oracle')))