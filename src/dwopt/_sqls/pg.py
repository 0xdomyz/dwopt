# db method


def list_tables(self):
    sql = (
        "select table_catalog,table_schema,table_name"
        "\n    ,is_insertable_into,commit_action"
        "\nfrom information_schema.tables"
        "\nwhere table_schema"
        "\n   not in ('information_schema','pg_catalog')"
    )
    return self.run(sql)


def table_cols(self, sch_tbl_nme):
    sch, tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
    sql = (
        "select column_name, data_type from information_schema.columns"
        f"\nwhere table_schema = '{sch}' "
        f"\nand table_name = '{tbl_nme}'"
    )
    return self.run(sql)


def list_cons(self):
    sql = "SELECT * FROM information_schema.constraint_table_usage"
    return self.run(sql)


# qry method
