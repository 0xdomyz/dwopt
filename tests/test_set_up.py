import dwopt
from sqlalchemy.engine import Engine
import os
import json

_DB_LST = ["pg", "lt", "oc"]


# todo add keyring test


def test_set_up_save_url_config(creds):
    for nme, url in zip(_DB_LST, creds):
        dwopt.save_url(nme, url, "config")
        url_back, _ = dwopt.set_up._get_url(nme)
        assert url_back == url

        dwopt.save_url(nme, url, "config", echo=True)
        url_back, kwargs = dwopt.set_up._get_url(nme)
        assert url_back == url
        assert kwargs["echo"] is True

        dwopt.save_url(nme, None, "config")
        url_back, _ = dwopt.set_up._get_url(nme)
        assert url_back != url


def test_set_up_save_url_environ(creds):
    for nme, url in zip(_DB_LST, creds):
        # url dict
        url_dict = {"url": url}
        os.environ[f"dwopt_{nme}"] = json.dumps(url_dict)
        url_back, _ = dwopt.set_up._get_url(nme)
        assert url_back == url

        # raw url
        os.environ[f"dwopt_{nme}"] = url
        url_back, _ = dwopt.set_up._get_url(nme)
        assert url_back == url

        url_dict = {"url": url, "echo": True}
        os.environ[f"dwopt_{nme}"] = json.dumps(url_dict)
        url_back, kwargs = dwopt.set_up._get_url(nme)
        assert url_back == url
        assert kwargs["echo"] is True


def test_set_up_save_url_ordering(creds):
    for nme, url in zip(_DB_LST, creds):
        dwopt.save_url(nme, url + "salt", "config")
        os.environ[f"dwopt_{nme}"] = url
        url_back, _ = dwopt.set_up._get_url(nme)
        assert url_back == url + "salt"


def test_set_up_make_eng():
    act = dwopt.make_eng("sqlite://")
    assert isinstance(act, Engine)
