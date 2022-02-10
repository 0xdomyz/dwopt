from dwopt import lt

def test_run(lt_tbl):
    lt.run('select * from test limit 1')

    run_select = lt.run('select * from test limit 1').iloc[0,].tolist()
    assert run_select == [0, 0.8444218515250481, 832, 'test', '2013-01-02']

    run_args = lt.run('select * from test where score > :score limit 2'
        ,args = {'score':'0.9'}).iloc[0,].tolist()
    assert run_args == [10, 0.9081128851953352, 738, 'test', '2013-02-02']

    run_mods = lt.run('select * from test where score > :score limit 2'
        ,score = 0.9).iloc[0,].tolist()
    assert run_mods == [10, 0.9081128851953352, 738, 'test', '2013-02-02']

