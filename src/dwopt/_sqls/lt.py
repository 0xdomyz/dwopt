#db method

def list_tables(self):
    sql = (
        "select * from sqlite_master "
        "\nwhere type ='table' "
        "\nand name NOT LIKE 'sqlite_%' "
    )
    return self.run(sql)