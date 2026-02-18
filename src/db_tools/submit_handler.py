from sqlalchemy import text
from deepdiff import DeepDiff
import logging
from .shared import DbToolsError

def get_tables(db_connection, db):
    try:
        db_connection.execute(text(f"USE `{db}`;"))
        result = db_connection.execute(text("SHOW TABLES;"))
        tables = [row[0] for row in result]
        return tables
    except Exception as e:
        raise DbToolsError(f"Failed to get tables from {db}: {e}")

def get_table_columns(db_connection, db, table):
    try:
        db_connection.execute(text(f"USE `{db}`;"))
        result = db_connection.execute(text(f"SHOW COLUMNS FROM `{table}`;"))
        columns = [(row[0], row[1], row[2], row[3], row[4],row[5]) for row in result]  # (name, type, extra)
        return columns
    except Exception as e:
        raise DbToolsError(f"Failed to get columns from {table}: {e}")

def get_table_count(db_connection, db, table, where_clause=None):
    try:
        db_connection.execute(text(f"USE `{db}`;"))
        sql = f"SELECT COUNT(*) FROM `{table}`"
        logging.debug(f"Executing SQL: {sql} with parameters: {where_clause}")
        if where_clause and where_clause.strip():
            sql += f" WHERE {where_clause.strip()}"
        result = db_connection.execute(text(sql))
        return result.scalar()
    except Exception as e:
        raise DbToolsError(f"Failed to get table count for {table}: {e}")
    
def compare_table_structure(src_cols, tgt_cols, src_constraints=None, tgt_constraints=None):
    """
    Compare table structure including columns, primary key, unique keys, and indices.
    Returns a tuple: (is_same, details)
    - is_same: True if structures are identical, False otherwise
    - details: dict describing the differences
    """
    details = {}

    diff = DeepDiff(src_cols, tgt_cols, ignore_order=True)
    if diff:
        details['columns'] = {
            "source": src_cols,
            "target": tgt_cols,
            "difference": diff
        }

    # # Compare columns
    # if src_cols != tgt_cols:
    #     details['columns'] = {
    #         "source": src_cols,
    #         "target": tgt_cols,
    #         "difference": "Columns are different"
    #     }

    # Compare constraints/indices if provided
    if src_constraints is not None and tgt_constraints is not None:
        # Primary Key
        if src_constraints.get("primary_key") != tgt_constraints.get("primary_key"):
            details['primary_key'] = {
                "source": src_constraints.get("primary_key"),
                "target": tgt_constraints.get("primary_key"),
                "difference": "Primary keys are different"
            }
        # Unique Keys
        if src_constraints.get("unique_keys") != tgt_constraints.get("unique_keys"):
            details['unique_keys'] = {
                "source": src_constraints.get("unique_keys"),
                "target": tgt_constraints.get("unique_keys"),
                "difference": "Unique keys are different"
            }
        # Indices
        if src_constraints.get("indices") != tgt_constraints.get("indices"):
            details['indices'] = {
                "source": src_constraints.get("indices"),
                "target": tgt_constraints.get("indices"),
                "difference": "Indices are different"
            }

    is_same = len(details) == 0
    return is_same, details

def compare_tables_handler(src_connection, tgt_connection, source_db, target_db, selected_tables, table_where_clauses=None):
    tgt_tables = get_tables(tgt_connection, target_db)
    result_rows = []
    table_where_clauses = table_where_clauses or {}
    for table in selected_tables:
        exists = table in tgt_tables if isinstance(tgt_tables, list) else False
        where_clause = table_where_clauses.get(table, None)
        logging.debug(f"Comparing table: {table}, Exists in target: {exists}, WHERE clause: {where_clause}")
        if not exists:
            row = (table, "❌ No", "-", "-")
        else:
            src_cols = get_table_columns(src_connection, source_db, table)
            tgt_cols = get_table_columns(tgt_connection, target_db, table)
            src_constraints = get_table_constraints_and_indices(src_connection, source_db, table)
            tgt_constraints = get_table_constraints_and_indices(tgt_connection, target_db, table)

            if isinstance(src_cols, str) or isinstance(tgt_cols, str):
                struct = "⚠️ Error"
            else:
                is_same, diff_details = compare_table_structure(src_cols, tgt_cols, src_constraints, tgt_constraints)
                struct = "✅ Same" if is_same else "⚠️ Different"
            src_count = get_table_count(src_connection, source_db, table, where_clause=where_clause)
            tgt_count = get_table_count(tgt_connection, target_db, table, where_clause=where_clause)
            if isinstance(src_count, str) or isinstance(tgt_count, str):
                row_count = "⚠️ Error"
            else:
                row_count = (
                    f"✅ Same ({src_count})"
                    if src_count == tgt_count
                    else f"⚠️ Different (src: {src_count}, tgt: {tgt_count})"
                )
            row = (table, "✅ Yes", struct, row_count)
        result_rows.append(row)
    return result_rows

def generate_alter_table_sql(src_cols, tgt_cols, table):
    """
    Generate SQL statements to alter the target table to match the source table structure.
    Only handles adding and dropping columns for simplicity.
    """
    src_col_names = set([col[0] for col in src_cols])
    tgt_col_names = set([col[0] for col in tgt_cols])

    # Columns to add and drop
    to_add = src_col_names - tgt_col_names
    to_drop = tgt_col_names - src_col_names

    # Find column definitions in source
    src_col_defs = {col[0]: col[1] for col in src_cols}

    statements = []
    for col in to_add:
        col_type = src_col_defs[col]
        statements.append(f"ALTER TABLE `{table}` ADD COLUMN `{col}` {col_type};")
    for col in to_drop:
        statements.append(f"ALTER TABLE `{table}` DROP COLUMN `{col}`;")

    return "\n".join(statements) if statements else "-- No changes needed"

def generate_data_sync_sql(src_connection, tgt_connection, source_db, target_db, table, key_columns=None):
    """
    Generate SQL statements to synchronize data from source to target table.
    - Only supports simple primary key-based upsert and insert for demonstration.
    - key_columns: list of column names to use as primary key for matching rows.
    """
    # Get columns and types for source table
    src_cols = get_table_columns(src_connection, source_db, table)
    tgt_cols = get_table_columns(tgt_connection, target_db, table)
    if isinstance(src_cols, str) or isinstance(tgt_cols, str):
        return "-- Error retrieving columns"

    # Use intersection of columns for data sync
    src_col_names = set([col[0] for col in src_cols])
    tgt_col_names = set([col[0] for col in tgt_cols])
    common_cols = list(src_col_names & tgt_col_names)
    if not common_cols:
        return "-- No common columns to sync"

    # If no key_columns provided, use the first column as a simple key (not recommended for production)
    if not key_columns:
        key_columns = [common_cols[0]]

    # Build the INSERT ... ON DUPLICATE KEY UPDATE statement
    cols_str = ", ".join(f"`{col}`" for col in common_cols)
    update_str = ", ".join(f"`{col}`=VALUES(`{col}`)" for col in common_cols if col not in key_columns)

    sql = (
        f"-- Sync data from `{source_db}`.`{table}` to `{target_db}`.`{table}`\n"
        f"INSERT INTO `{target_db}`.`{table}` ({cols_str})\n"
        f"SELECT {cols_str} FROM `{source_db}`.`{table}`\n"
        f"ON DUPLICATE KEY UPDATE {update_str};"
    )
    return sql

def get_table_constraints_and_indices(conn, db, table):
    # Primary Key
    pk_result = conn.execute(text(f"SHOW KEYS FROM `{table}` IN `{db}` WHERE Key_name = 'PRIMARY';"))
    pk = [row['Column_name'] if 'Column_name' in row else row[4] for row in pk_result]

    # Unique Keys
    unique_result = conn.execute(text(f"SHOW KEYS FROM `{table}` IN `{db}` WHERE Non_unique = 0 AND Key_name != 'PRIMARY';"))
    unique = {}
    for row in unique_result:
        key_name = row['Key_name'] if 'Key_name' in row else row[2]
        col_name = row['Column_name'] if 'Column_name' in row else row[4]
        unique.setdefault(key_name, []).append(col_name)

    # Indices (non-unique)
    index_result = conn.execute(text(f"SHOW KEYS FROM `{table}` IN `{db}` WHERE Non_unique = 1;"))
    indices = {}
    for row in index_result:
        key_name = row['Key_name'] if 'Key_name' in row else row[2]
        col_name = row['Column_name'] if 'Column_name' in row else row[4]
        indices.setdefault(key_name, []).append(col_name)

    return {
        "primary_key": pk,
        "unique_keys": unique,
        "indices": indices
    }

# Example usage:
# src_cols = [('id', 'int(11)'), ('name', 'varchar(255)')]
# tgt_cols = [('id', 'int(11)')]
# print(generate_alter_table_sql(src_cols, tgt_cols, 'mytable'))
# print(generate_data_sync_sql(db_connection, "src_db", "tgt_db", "mytable", key_columns=["id"]))