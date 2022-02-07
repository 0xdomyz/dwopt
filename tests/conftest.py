from dwops import lt
import pandas as pd
import random
import pytest

@pytest.fixture(scope="session")
def lt_tbl():
    
    n = 10000
    random.seed(0)
    df = pd.DataFrame(
        {
            "id": range(n)
            ,"score": [random.random() for i in range(n)]
            ,"amt": [random.choice(range(1000)) for i in range(n)]
            ,"cat": [random.choice(["test", "train"]) for i in range(n)]
            ,'time': [
                    random.choice(
                        [pd.Timestamp(i) for i in 
                            ["20130102","20130202","20130302"]]
                    ) 
                    for i in range(n)]
        }
    )

    lt.drop('test')
    lt.create(
            tbl_nme = 'test'
            ,dtypes = {
                'id':'integer'
                ,'score':'real'
                ,'amt':'integer'
                ,'cat':'text'
                ,'time':'text'
                ,'constraint df_pk':
                    'primary key (id)'
            }
        )
    lt.write(df,'test')

    return None
