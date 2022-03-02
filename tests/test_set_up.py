import dwopt
from sqlalchemy.engine import Engine
import os


def test_urls_save_url_config(fix_credential):
    for nme, url in zip(["pg", "lt", "oc"], fix_credential):
        dwopt.save_url(nme, url, "config")
        assert dwopt.set_up._get_url(nme) == url


def test_urls_save_url_environ(fix_credential):
    for nme, url in zip(["pg", "lt", "oc"], fix_credential):
        os.environ[f"dwopt_{nme}"] = url
        assert dwopt.set_up._get_url(nme) == url


def test_urls_save_url_ordering(fix_credential):
    for nme, url in zip(["pg", "lt", "oc"], fix_credential):
        dwopt.save_url(nme, url + "salt", "config")
        os.environ[f"dwopt_{nme}"] = url
        assert dwopt.set_up._get_url(nme) == url + "salt"


def test_urls_make_eng():
    act = dwopt.make_eng("sqlite://")
    assert isinstance(act, Engine)
