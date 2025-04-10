import polars as pl


def add_ID(table: pl.DataFrame) -> pl.DataFrame:
    """Adds column with ids starting at 1"""
    return table.with_row_index("id", offset=1)


def remove_column(table: pl.DataFrame, row: str) -> pl.DataFrame:
    """Remove column with given name"""
    return table.drop(row)


def change_to_foreign_ID(
    table: pl.DataFrame,
    foreign_table: pl.DataFrame,
    old_column: str,
    new_column: str,
    old_foreign_column: str,
    new_foreign_column: str = "id",
) -> pl.DataFrame:
    """Changes reference to other table to use proper id column

    Args:
        table (pl.DataFrame): The table refering to another table
        foreign_table (pl.DataFrame): The table referred to
        old_column (str): The column which used to refer to the other table
        new_column (str): Name to call new column referring to other table
        old_foreign_column (str): Name of the column previously referred to
        new_foreign_column (str, optional): Name of preferred column to refer to. Defaults to "id".

    Returns:
        pl.DataFrame: Updated dataframe
    """
    # Look only at the two relevant columns of foreign table
    relevant_foreign_columns = foreign_table[[old_foreign_column, new_foreign_column]]
    # Look only at the one relevant column of local table
    relevant_local_column = table[[old_column]]
    # Generate column of id values corresponding to local values in limitted foreign table
    matching_ids = relevant_local_column.join(
        relevant_foreign_columns, how="left", left_on=old_column, right_on=old_foreign_column
    )[new_foreign_column]
    # Add id column to local table with chosen name
    table = table.with_columns(matching_ids.alias(new_column))
    # Remove old column if it hasn't been already by adding
    if old_column != new_column:
        table = remove_column(table, old_column)
    return table


def replace_values_in_column(
    table: pl.DataFrame, column: str, old, new
) -> pl.DataFrame:
    """Replace values in column where they currently has given value"""
    new_column = table[column].replace(old, new)
    index = table.get_column_index(column)
    return table.replace_column(index, new_column)


def round_floats(table: pl.DataFrame, column: str, decimals: int = 2) -> pl.DataFrame:
    """Round floating values in column to number of decimals"""
    return table.with_columns(pl.col(column).round(decimals))


def change_data_type(
    table: pl.DataFrame, column: str, type: pl.DataType
) -> pl.DataFrame:
    """Change the datatype of column"""
    return table.with_columns(pl.col(column).cast(type, strict=False))


def change_column_name(
    table: pl.DataFrame, old_name: str, new_name: str
) -> pl.DataFrame:
    """Change name of column"""
    return table.rename({old_name: new_name})


def split_prepended(
        table: pl.DataFrame,
        column: str,
        nr_column_name: str,
        **kwargs: str
) -> pl.DataFrame:
    "Move prepended substring from string to separate column"
    if kwargs.get("rest_column_name") is None:
        rest_column_name = column
    
    table = table.with_columns(
        pl.col(column)
        .str.splitn(" ", 2)
        .struct.rename_fields([nr_column_name, rest_column_name])
        .alias("splits")
    )
    table = remove_column(table, column)
    return table.unnest("splits")

def remove_surrounding(table: pl.DataFrame, column: str) -> pl.DataFrame:
    "Removes surrounding whitespace from column"
    #For some reason does not recognize str.strip() anymore?
    return table.with_columns(pl.col(column).str.strip_chars_end().str.strip_chars_start())



def main():
    df = pl.DataFrame({"foo": [" hello  ", "\nworld"], "bar": ["goodbye", "world "]})
    print(remove_surrounding(df, "foo"))


if __name__ == "__main__":
    main()
