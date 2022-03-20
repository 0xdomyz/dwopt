import dwopt
from sqlalchemy.engine import Engine
import os

_DB_LST = ["pg", "lt", "oc"]


def test_set_up_save_url_config(creds):
    for nme, url in zip(_DB_LST, creds):
        dwopt.save_url(nme, url, "config")
        assert dwopt.set_up._get_url(nme) == url


def test_set_up_save_url_environ(creds):
    for nme, url in zip(_DB_LST, creds):
        os.environ[f"dwopt_{nme}"] = url
        assert dwopt.set_up._get_url(nme) == url


def test_set_up_save_url_ordering(creds):
    for nme, url in zip(_DB_LST, creds):
        dwopt.save_url(nme, url + "salt", "config")
        os.environ[f"dwopt_{nme}"] = url
        assert dwopt.set_up._get_url(nme) == url + "salt"


def test_set_up_make_eng():
    act = dwopt.make_eng("sqlite://")
    assert isinstance(act, Engine)
