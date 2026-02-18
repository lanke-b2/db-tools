import streamlit as st
import pandas as pd
import json
import os
from sqlalchemy import create_engine, text
from db_tools.submit_handler import (
    get_tables,
    get_table_columns,
    get_table_constraints_and_indices,
    compare_table_structure,
    generate_alter_table_sql,
    get_table_count,
)
from db_tools.content_compare import (
    compare_table_content,
    generate_content_sync_sql,
)
from db_tools.shared import (
    load_connections,
    save_connections,
)

st.set_page_config(page_title="DB Tools Web", layout="wide")
st.title("DB Tools - Web Edition")

connections = load_connections()

# --- Sidebar: Connection ---
st.sidebar.header("Source Database Connection")

# Profile selection
profile_names = list(connections.keys())
selected_profile = st.sidebar.selectbox("Connection Profile", ["New Connection"] + profile_names)

if selected_profile != "New Connection" and selected_profile in connections:
    profile_data = connections[selected_profile]
    host = st.sidebar.text_input("Host", value=profile_data.get("host", "127.0.0.1"))
    port = st.sidebar.text_input("Port", value=profile_data.get("port", "3306"))
    username = st.sidebar.text_input("Username", value=profile_data.get("username", "admin"))
    # Retrieve password from keyring
    import keyring
    password = st.sidebar.text_input("Password", value=keyring.get_password("db-tools", f"{selected_profile}_password") or "", type="password")
else:
    host = st.sidebar.text_input("Host", value="127.0.0.1")
    port = st.sidebar.text_input("Port", value="3306")
    username = st.sidebar.text_input("Username", value="admin")
    password = st.sidebar.text_input("Password", value="", type="password")

# Save profile
if st.sidebar.button("Save Profile"):
    profile_name = st.sidebar.text_input("Profile Name", key="save_profile")
    if profile_name:
        connections[profile_name] = {"host": host, "port": port, "username": username}
        save_connections(connections)
        # Save password to keyring
        import keyring
        keyring.set_password("db-tools", f"{profile_name}_password", password)
        st.sidebar.success(f"Profile '{profile_name}' saved!")
        st.rerun()

# Target connection option
use_different_target = st.sidebar.checkbox("Use different target connection")
target_engine = None

if use_different_target:
    st.sidebar.header("Target Database Connection")
    target_profile = st.sidebar.selectbox("Target Profile", ["New Connection"] + profile_names, key="target_profile")
    
    if target_profile != "New Connection" and target_profile in connections:
        target_data = connections[target_profile]
        target_host = st.sidebar.text_input("Target Host", value=target_data.get("host", "127.0.0.1"))
        target_port = st.sidebar.text_input("Target Port", value=target_data.get("port", "3306"))
        target_username = st.sidebar.text_input("Target Username", value=target_data.get("username", "admin"))
        target_password = st.sidebar.text_input("Target Password", value=target_data.get("password", ""), type="password")
    else:
        target_host = st.sidebar.text_input("Target Host", value="127.0.0.1")
        target_port = st.sidebar.text_input("Target Port", value="3306")
        target_username = st.sidebar.text_input("Target Username", value="admin")
        target_password = st.sidebar.text_input("Target Password", value="", type="password")

if st.sidebar.button("Connect"):
    try:
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/")
        with engine.connect() as conn:
            dbs = [row[0] for row in conn.execute(text("SHOW DATABASES;"))]
        st.session_state['engine'] = engine
        st.session_state['dbs'] = dbs
        
        if use_different_target:
            target_engine = create_engine(f"mysql+pymysql://{target_username}:{target_password}@{target_host}:{target_port}/")
            with target_engine.connect() as conn:
                target_dbs = [row[0] for row in conn.execute(text("SHOW DATABASES;"))]
            st.session_state['target_engine'] = target_engine
            st.session_state['target_dbs'] = target_dbs
        else:
            st.session_state['target_engine'] = engine
            st.session_state['target_dbs'] = dbs
            
        st.success("Connected!")
    except Exception as e:
        st.error(f"Connection failed: {e}")

if 'engine' in st.session_state:
    engine = st.session_state['engine']
    target_engine = st.session_state['target_engine']
    dbs = st.session_state['dbs']
    target_dbs = st.session_state['target_dbs']

    # --- Database Selection ---
    st.sidebar.header("Database Selection")
    source_db = st.sidebar.selectbox("Source Database", dbs, key="src_db")
    target_db = st.sidebar.selectbox("Target Database", target_dbs, key="tgt_db")

    # --- Table Selection ---
    with engine.connect() as src_conn, target_engine.connect() as tgt_conn:
        src_tables = get_tables(src_conn, source_db)
        tgt_tables = get_tables(tgt_conn, target_db)
    selected_tables = st.multiselect("Select Tables to Compare", src_tables)

    # --- WHERE Clauses ---
    where_clauses = {}
    for table in selected_tables:
        where_clauses[table] = st.text_area(f"WHERE clause for `{table}` (optional)", key=f"where_{table}")

    # --- Compare Button ---
    if st.button("Compare"):
        with engine.connect() as src_conn, target_engine.connect() as tgt_conn:
            results = []
            for table in selected_tables:
                src_cols = get_table_columns(src_conn, source_db, table)
                tgt_cols = get_table_columns(tgt_conn, target_db, table)
                src_constraints = get_table_constraints_and_indices(src_conn, source_db, table)
                tgt_constraints = get_table_constraints_and_indices(tgt_conn, target_db, table)
                is_same, struct_diff = compare_table_structure(src_cols, tgt_cols, src_constraints, tgt_constraints)
                where_clause = where_clauses.get(table, None)
                src_count = get_table_count(src_conn, source_db, table, where_clause=where_clause)
                tgt_count = get_table_count(tgt_conn, target_db, table, where_clause=where_clause)
                content_status = (
                    f"✅ Same ({src_count})" if src_count == tgt_count else f"⚠️ Different (src: {src_count}, tgt: {tgt_count})"
                )
                results.append({
                    "table": table,
                    "structure": "✅ Same" if is_same else "⚠️ Different",
                    "structure_diff": struct_diff,
                    "content": content_status,
                    "src_cols": src_cols,
                    "tgt_cols": tgt_cols,
                    "src_constraints": src_constraints,
                    "tgt_constraints": tgt_constraints,
                })
        st.session_state['results'] = results
        st.session_state['source_db'] = source_db
        st.session_state['target_db'] = target_db
        st.session_state['where_clauses'] = where_clauses

    # --- Results Table ---
    if 'results' in st.session_state:
        st.subheader("Comparison Results")
        for res in st.session_state['results']:
            st.markdown(f"### Table: `{res['table']}`")
            st.write("**Structure:**", res["structure"])
            if res["structure"] == "⚠️ Different":
                with st.expander("Show Structure Differences"):
                    import pandas as pd
                    
                    # Get all column names from both source and target
                    src_cols = {col[0]: f"{col[1]} {col[5]}" for col in res["src_cols"]}
                    tgt_cols = {col[0]: f"{col[1]} {col[5]}" for col in res["tgt_cols"]}
                    all_cols = sorted(set(src_cols.keys()) | set(tgt_cols.keys()))
                    
                    # Create comparison data
                    comparison_data = []
                    for col in all_cols:
                        src_val = src_cols.get(col, "(missing)")
                        tgt_val = tgt_cols.get(col, "(missing)")
                        comparison_data.append({
                            "Column": col,
                            "Source": src_val,
                            "Target": tgt_val
                        })
                    
                    df = pd.DataFrame(comparison_data)
                    
                    # Apply styling based on source vs target comparison
                    def highlight_diff(row):
                        if row['Source'] != row['Target']:
                            return ['background-color: #ffcccc'] * 3
                        return [''] * 3
                    
                    styled_df = df.style.apply(highlight_diff, axis=1)
                    st.dataframe(styled_df, use_container_width=True)
            st.write("**Content:**", res["content"])
            if st.button(f"Generate Upgrade Script for `{res['table']}`", key=f"upgrade_{res['table']}"):
                with engine.connect() as src_conn, target_engine.connect() as tgt_conn:
                    auto_inc_cols = [col[0] for col in res["src_cols"] if "auto_increment" in str(col[5]).lower()]
                    alter_sql = generate_alter_table_sql(res["src_cols"], res["tgt_cols"], res["table"])
                    
                    # Use a single connection for content comparison (source connection with cross-database queries)
                    diff = compare_table_content(
                        src_conn, tgt_conn, st.session_state['source_db'], st.session_state['target_db'], res["table"],
                        source_where=st.session_state['where_clauses'].get(res["table"]),
                        target_where=st.session_state['where_clauses'].get(res["table"])
                    )
                    if isinstance(diff, dict) and "error" not in diff:
                        data_sql = generate_content_sync_sql(
                            diff["col_names"], diff["missing_in_target"], diff["missing_in_source"], 
                            res["table"], diff["values_different"], diff["pk"], auto_inc_cols=auto_inc_cols
                        )
                    else:
                        data_sql = "-- Error: Cannot compare content across different connections"
                    st.code(f"-- Structure Upgrade\n{alter_sql}\n\n-- Data Sync\n{data_sql}", language="sql")