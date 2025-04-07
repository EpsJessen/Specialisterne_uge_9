import polars as pl


def add_ID(table: pl.DataFrame) -> pl.DataFrame:
    return table.with_row_index("id", offset=1)


def remove_column(table: pl.DataFrame, row: str) -> pl.DataFrame:
    return table.drop(row)


def change_to_foreign_ID(
    table: pl.DataFrame,
    foreign_table: pl.DataFrame,
    old_column: str,
    new_column: str,
    old_foreign_column: str,
    new_foreign_column: str = "id",
) -> pl.DataFrame:
    limit_ft = foreign_table[[old_foreign_column, new_foreign_column]]
    limit_t = table[[old_column]]
    matches = limit_t.join(
        limit_ft, how="left", left_on=old_column, right_on=old_foreign_column
    )[new_foreign_column]
    table = table.with_columns(matches.alias(new_column))
    if old_column != new_column:
        table = remove_column(table, old_column)
    return table


def replace_values_in_column(
    table: pl.DataFrame, column: str, old, new
) -> pl.DataFrame:
    new_column = table[column].replace(old, new)
    index = table.get_column_index(column)
    return table.replace_column(index, new_column)


def round_floats(table: pl.DataFrame, column: str, decimals: int = 2) -> pl.DataFrame:
    return table.with_columns(pl.col(column).round(decimals))


def change_data_type(
    table: pl.DataFrame, column: str, type: pl.DataType
) -> pl.DataFrame:
    return table.with_columns(pl.col(column).cast(type, strict=False))


def change_column_name(
    table: pl.DataFrame, old_name: str, new_name: str
) -> pl.DataFrame:
    return table.rename({old_name: new_name})


def main():
    pass


if __name__ == "__main__":
    main()
