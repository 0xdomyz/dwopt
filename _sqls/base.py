def head(self):
    return self.run("select * from x limit 5")

def top(self):
    return self.run("select * from x limit 1").iloc[0,]

def cols(self):
    return self.run("select * from x where 1=2").columns.tolist()

def len(self):
    return self.run("select count(1) from x").iloc[0,0]

def dist(self,*args):
    _ = (" || '_' || ".join(_) if not isinstance(_,str) else _ 
        for _ in args)
    _ = ''.join(
        f"    ,count(distinct {j})\n" 
        if i else 
        f'    count(distinct {j})\n'
        for i,j in enumerate(_)
    )
    _ = (
        "select \n"
        f'{_}'
        'from x'
    )
    return self.run(_).iloc[0,:]

def mimx(self,col):
    _ = (
        "select \n"
        f"    max({col}),min({col})\n"
        "from x"
    )
    return self.run(_).iloc[0,:]

def valc(self,group_by,agg = None,order_by = None,n = True):
    group_by_cls = (','.join(group_by) if not isinstance(group_by,str)
        else group_by)
    if agg is None:
        agg_cls = ''
    elif isinstance(agg,str):
        agg_cls = f"    ,{agg}\n"
    else:
        agg_cls = ''.join(f"    ,{_}\n" for _ in agg)
    if order_by is None:
        if n:
            order_by_cls = 'n desc'
        else:
            order_by_cls = group_by_cls
    else:
        order_by_cls = order_by
    _ = (
        "select \n"
        f"    {group_by_cls}\n"
        f"{f'    ,count(1) n{chr(10)}' if n else ''}"
        f"{agg_cls}"
        "from x\n"
        f"group by {group_by_cls}\n"
        f"order by {order_by_cls}"
    )
    return self.run(_)




