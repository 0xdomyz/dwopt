import dwopt
from sqlalchemy.engine import Engine
import os


def test_urls_save_url_config(fix_credential_clean_up):
    for nme, url in zip(['pg', 'lt', 'oc'], fix_credential_clean_up):
        dwopt.save_url(nme, url, 'config')
        assert dwopt.urls._get_url(nme) == url


def test_urls_save_url_environ(fix_credential_clean_up):
    for nme, url in zip(['pg', 'lt', 'oc'], fix_credential_clean_up):
        os.environ[f'dwopt_{nme}'] = url
        assert dwopt.urls._get_url(nme) == url


def test_urls_save_url_ordering(fix_credential_clean_up):
    for nme, url in zip(['pg', 'lt', 'oc'], fix_credential_clean_up):
        dwopt.save_url(nme, url + 'salt', 'config')
        os.environ[f'dwopt_{nme}'] = url
        assert dwopt.urls._get_url(nme) == url + 'salt'


def test_urls_make_eng():
    act = dwopt.make_eng('sqlite://')
    assert isinstance(act, Engine)

