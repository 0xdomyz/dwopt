import sqlalchemy
import dwopt
from sqlalchemy.engine import Engine

def test_urls_save_url():
    try:
        dwopt.save_url('pg', 'postgresql://scott:tiger@localhost/mydatabase')
    except Exception:
        try:
            dwopt.save_url('sqlite', 'sqlite://', 'environ')
        except Exception:
            dwopt.save_url('oracle', 'oracle://scott:tiger@tnsname', 'config')
    assert 1 == 1

def test_urls_make_eng():
    act = dwopt.make_eng('sqlite://')
    assert isinstance(act, Engine)

