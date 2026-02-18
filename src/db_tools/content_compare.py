from sqlalchemy import text
from deepdiff import DeepDiff
import logging
from .shared import DbToolsError

def get_table_columns(db_connection, db, table):
    """
    Returns a list of tuples: (column_name, column_type, extra_info)
    """
    try:
        db_connection.execute(text(f"USE `{db}`;"))
        result = db_connection.execute(text(f"SHOW COLUMNS FROM `{table}`;"))
        columns = [(row[0], row[1], row[5]) for row in result]  # (name, type, extra)
        return columns
    except Exception as e:
        raise DbToolsError(f"Failed to get columns for table {table} in db {db}: {e}")

def get_table_rows(db_connection, db, table, columns, where_clause=None):
    """
    Returns (col_names, rows) for all columns (including auto_increment/PK columns).
    """
    try:
        db_connection.execute(text(f"USE `{db}`;"))
        col_names = [col[0] for col in columns ]
        col_str = ", ".join(f"`{c}`" for c in col_names)
        sql = f"SELECT {col_str} FROM `{table}`"
        logging.info(f"Executing SQL: {sql} with parameters: {where_clause}")
        if where_clause and where_clause.strip():
            sql += f" WHERE {where_clause.strip()}"
        result = db_connection.execute(text(sql))
        rows = [tuple(row) for row in result]
        return col_names, rows
    except Exception as e:
        raise DbToolsError(f"Failed to get rows for table {table} in db {db}: {e}")

def get_primary_key(db_connection, db, table):
    """
    Returns the primary key column name(s) for the given table.
    Returns a string for single PK, or a list for composite PKs, or None if no PK.
    """
    try:
        db_connection.execute(text(f"USE `{db}`;"))
        result = db_connection.execute(
            text(f"SHOW KEYS FROM `{table}` WHERE Key_name = 'PRIMARY';")
        )
        pk_cols = []
        for row in result:
            # Try both dict and tuple access
            if isinstance(row, dict) or hasattr(row, 'keys'):
                col = row.get('Column_name') or row.get('column_name')
                if not col and hasattr(row, '_mapping'):
                    col = row._mapping.get('Column_name') or row._mapping.get('column_name')
            else:
                # Fallback to positional index (Column_name is usually at index 4)
                col = row[4] if len(row) > 4 else None
            if col:
                pk_cols.append(col)
        if not pk_cols:
            return None
        return pk_cols[0] if len(pk_cols) == 1 else pk_cols
    except Exception as e:
        raise DbToolsError(f"Failed to get primary key for table {table} in db {db}: {e}")

def compare_table_content(
    src_conn, tgt_conn, source_db, target_db, table, 
    source_where=None, target_where=None
):
    """
    Compare table content between source and target, using the actual PK from metadata.
    Optional source_where and target_where clauses can be provided.
    If PK is auto_increment, exclude it from content comparison.
    """
    logging.info(f"Source WHERE: {source_where}, Target WHERE: {target_where}")
    src_cols = get_table_columns(src_conn, source_db, table)
    tgt_cols = get_table_columns(tgt_conn, target_db, table)

    diff = DeepDiff(src_cols, tgt_cols, ignore_order=True)
    if diff:
        raise DbToolsError("Table structure is not identical")

    # Get PK from metadata
    src_pk = get_primary_key(src_conn, source_db, table)
    if not src_pk:
        raise DbToolsError("No primary key found in table")

    # Identify auto_increment columns
    auto_inc_cols = [col[0] for col in src_cols if "auto_increment" in col[2].lower()]

    # Get rows (skip auto_increment columns for content comparison, but keep PK for mapping)
    col_names, src_rows = get_table_rows(
        src_conn, source_db, table, src_cols, where_clause=source_where
    )
    _, tgt_rows = get_table_rows(
        tgt_conn, target_db, table, tgt_cols, where_clause=target_where
    )

    # Support composite PKs
    def pk_tuple(row):
        if isinstance(src_pk, list):
            idxs = [col_names.index(k) for k in src_pk]
            return tuple(row[i] for i in idxs)
        else:
            return row[col_names.index(src_pk)]

    src_dict = {pk_tuple(row): row for row in src_rows}
    tgt_dict = {pk_tuple(row): row for row in tgt_rows}

    missing_in_target = [dict(zip(col_names, src_dict[k])) for k in src_dict if k not in tgt_dict]
    missing_in_source = [dict(zip(col_names, tgt_dict[k])) for k in tgt_dict if k not in src_dict]
    values_different = []
    # Columns to compare (exclude auto_increment PK columns)
    compare_cols = [c for c in col_names if c not in auto_inc_cols]

    for k in src_dict:
        if k in tgt_dict:
            src_row = src_dict[k]
            tgt_row = tgt_dict[k]
            # Compare only non-auto_increment columns
            src_comp = [src_row[col_names.index(c)] for c in compare_cols]
            tgt_comp = [tgt_row[col_names.index(c)] for c in compare_cols]
            if src_comp != tgt_comp:
                values_different.append({
                    "pk": k,
                    "source": dict(zip(col_names, src_row)),
                    "target": dict(zip(col_names, tgt_row))
                })

    return {
        "missing_in_target": missing_in_target,
        "missing_in_source": missing_in_source,
        "values_different": values_different,
        "pk": src_pk,
        "col_names": col_names
    }

def generate_content_sync_sql(col_names, missing_in_target, missing_in_source, table, values_different=None, pk=None, auto_inc_cols=None):
    """
    Generate SQL to sync content:
    - Insert missing_in_target into target
    - Delete missing_in_source from target
    - Update values_different in target
    Excludes auto-increment columns from INSERT/UPDATE.
    Supports composite primary keys.
    """
    auto_inc_cols = auto_inc_cols or []
    filtered_col_names = [c for c in col_names if c not in auto_inc_cols]
    stmts = []
    # Insert statements
    for row in missing_in_target:
        cols = ", ".join(f"`{c}`" for c in filtered_col_names)
        vals = ", ".join(f"'{str(row[c]).replace('\'', '\\\'')}'" if row[c] is not None else "NULL" for c in filtered_col_names)
        stmts.append(f"INSERT INTO `{table}` ({cols}) VALUES ({vals});")
    # Delete statements (use PK columns for WHERE)
    for row in missing_in_source:
        if isinstance(pk, list):
            where = " AND ".join(
                f"`{c}`={'NULL' if row[c] is None else f'\'{str(row[c]).replace('\'', '\\\'')}\''}" for c in pk
            )
        else:
            where = f"`{pk}`={'NULL' if row[pk] is None else f'\'{str(row[pk]).replace('\'', '\\\'')}\''}"
        stmts.append(f"DELETE FROM `{table}` WHERE {where};")
    # Update statements for values_different
    if values_different and pk:
        for diff in values_different:
            # Exclude PK columns from SET clause
            if isinstance(pk, list):
                set_clause = ", ".join(
                    f"`{c}`={'NULL' if diff['source'][c] is None else f'\'{str(diff['source'][c]).replace('\'', '\\\'')}\''}"
                    for c in filtered_col_names if c not in pk
                )
                where = " AND ".join(
                    f"`{c}`={'NULL' if diff['source'][c] is None else f'\'{str(diff['source'][c]).replace('\'', '\\\'')}\''}" for c in pk
                )
            else:
                set_clause = ", ".join(
                    f"`{c}`={'NULL' if diff['source'][c] is None else f'\'{str(diff['source'][c]).replace('\'', '\\\'')}\''}"
                    for c in filtered_col_names if c != pk
                )
                where = f"`{pk}`={'NULL' if diff['source'][pk] is None else f'\'{str(diff['source'][pk]).replace('\'', '\\\'')}\''}"
            stmts.append(f"UPDATE `{table}` SET {set_clause} WHERE {where};")
    return "\n".join(stmts) if stmts else "-- No content sync needed"

# Example usage:
# diff = compare_table_content(db_connection, "src_db", "tgt_db", "mytable")
# sql = generate_content_sync_sql(diff["col_names"], diff["missing_in_target"], diff["missing_in_source"], "mytable", diff["values_different"], diff["pk"])