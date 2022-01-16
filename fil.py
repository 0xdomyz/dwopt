import pandas as pd
import yaml
import logging
import os

_logger = logging.getLogger(__name__)
_pth = os.path.dirname(__file__)

def get(nme, pth = _pth):
    fp = f'{pth}\\{nme}'
    ext = nme.split('.')[1]
    _logger.info(f'read {fp}')
    if ext == 'csv':
        a = pd.read_csv(fp)
    elif ext == 'pkl':
        a = pd.read_pickle(fp)
    elif ext == 'txt':
        with open(fp) as f:
            a = f.read()
    elif ext == 'yaml':
        with open(fp) as f:
            a = pd.DataFrame(yaml.safe_load(f))
    else:
        raise Exception('ext not recognised')
    _logger.info(f'len: {len(a)}')
    return a

def set(a, nme, pth = _pth):
    fp = f'{pth}\\{nme}'
    ext = nme.split('.')[1]
    _logger.info(f'save to {fp}, len: {len(a)}')
    if ext == 'csv':
        a.to_csv(fp,index=False)
    elif ext == 'pkl':
        a.to_pickle(fp)
    elif ext == 'txt':
        with open(fp,'w') as f:
            f.write(a)
    elif ext == 'yaml':
        with open(fp,'w') as f:
            _ = yaml.dump(a.to_dict('records'))
            f.write(_)
    else:
        raise Exception('ext not recognised')

def get_key(nme, pth = _pth):
    fp = f'{pth}\\key\\{nme}.txt'
    L = open(fp,"r").readlines()
    return (L[0][:-1], L[1][:-1])

def get_pth(nme, pth = _pth):
    return f'{pth}\\{nme}'


