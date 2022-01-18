def head(self):
    return self.run("select * from x where rownum<=5")

def first(self):
    return self.run("select * from x where rownum<=1")

def hash(self,*args):
    if len(args) == 0:
        args = self.cols()
    _ = args[0] if len(args) == 1 and not isinstance(args[0],str) else args
    _ = " || '_' || ".join(_)
    _ = (
        "select/*+ PARALLEL(4) */ \n"
        "    ora_hash(sum(ora_hash(\n"
        f"        {_}\n"
        "    ) - 4294967296/2)) hash\n"
        "from x"
    )
    return self.run(_).iloc[0,0]

