#db method

def list_tables(self):
    sql = (
        "select * from sqlite_schema "
        "\nwhere type ='table' "
        "\nand name NOT LIKE 'sqlite_%' "
    )
    return self.run(sql)