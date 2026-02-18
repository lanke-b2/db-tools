import os
import json
import re
import logging
import tkinter as tk
from tkinter import messagebox, ttk, simpledialog
from sqlalchemy import create_engine, text
from .submit_handler import (
    compare_tables_handler,
    generate_alter_table_sql,
    get_tables,
    get_table_columns,
    get_table_constraints_and_indices
)
from .content_compare import (
    compare_table_content,
    generate_content_sync_sql
)
from .shared import (
    load_connections,
    save_connections,
    DbToolsError
)


logging.basicConfig(level=logging.INFO)
logging.info("Logger initialized")

connections = load_connections()
profile_names = list(connections.keys())

def get_mysql_databases(db_connection):
    try:
        result = db_connection.execute(text("SHOW DATABASES;"))
        return [row[0] for row in result]
    except Exception as e:
        raise DbToolsError(f"Failed to get databases: {e}")

def connect_and_next():
    host = host_var.get()
    port = port_var.get()
    username = username_var.get()
    password = password_var.get()

    try:
        global db_engine
        db_engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/")
        global db_connection
        db_connection = db_engine.connect()
    except Exception as e:
        messagebox.showerror("Connection Error", f"Failed to connect: {str(e)}")
        return

    # Handle target connection
    global target_engine, target_connection
    if use_different_target_var.get():
        target_host = target_host_var.get()
        target_port = target_port_var.get()
        target_username = target_username_var.get()
        target_password = target_password_var.get()
        try:
            target_engine = create_engine(f"mysql+pymysql://{target_username}:{target_password}@{target_host}:{target_port}/")
            target_connection = target_engine.connect()
        except Exception as e:
            messagebox.showerror("Target Connection Error", f"Failed to connect to target: {str(e)}")
            return
    else:
        target_engine = db_engine
        target_connection = db_connection

    try:
        dbs = get_mysql_databases(db_connection)
        target_dbs = get_mysql_databases(target_connection)
    except DbToolsError as e:
        messagebox.showerror("Connection Error", str(e))
        return
    
    if not dbs:
        messagebox.showwarning("No Databases", "No databases found.")
        return
        
    connection_frame.pack_forget()
    schema_frame.pack(fill="both", expand=True)
    db_list_var.set("\n".join(dbs))
    source_db_combo['values'] = dbs
    target_db_combo['values'] = target_dbs
    if dbs:
        source_db_combo.current(0)
    if target_dbs:
        target_db_combo.current(0)
    source_tables_listbox.delete(0, tk.END)

def update_source_tables(event=None):
    db = source_db_var.get()
    try:
        tables = get_tables(db_connection, db)
        source_tables_listbox.delete(0, tk.END)
        for t in tables:
            source_tables_listbox.insert(tk.END, t)
    except DbToolsError as e:
        messagebox.showerror("Error", str(e))

def update_target_tables(event=None):
    pass

def submit():
    source_db = source_db_var.get()
    target_db = target_db_var.get()
    selected_indices = source_tables_listbox.curselection()
    selected_tables = [source_tables_listbox.get(i) for i in selected_indices]
    if not selected_tables:
        messagebox.showwarning("No Tables Selected", "Please select at least one source table.")
        return
    # Collect latest WHERE clauses from text widgets
    for table, widget in where_clause_text_widgets.items():
        table_where_clauses[table] = widget.get("1.0", tk.END).strip()
    # Pass table_where_clauses to result table
    try:
        result_rows = compare_tables_handler(
            db_connection, target_connection, source_db, target_db, selected_tables, dict(table_where_clauses)
        )
        show_result_table(result_rows, selected_tables, dict(table_where_clauses))
    except DbToolsError as e:
        messagebox.showerror("Error", str(e))

def show_content_diff_window(table_name, diff_json):
    win = tk.Toplevel()
    win.title(f"Content Differences for {table_name}")
    script_window_width = int(700 * 1.3)
    script_window_height = 500
    screen_width = win.winfo_screenwidth()
    screen_height = win.winfo_screenheight()
    x = int((screen_width / 2) - (script_window_width / 2))
    y = int((screen_height / 2) - (script_window_height / 2))
    win.geometry(f"{script_window_width}x{script_window_height}+{x}+{y}")
    txt = tk.Text(win, wrap="word")
    txt.insert("1.0", json.dumps(diff_json, indent=2, default=str))
    txt.pack(expand=True, fill="both", padx=10, pady=10)
    tk.Button(win, text="Close", command=win.destroy).pack(pady=5)

def show_structure_diff_window(table_name, src_cols, tgt_cols, src_constraints, tgt_constraints):
    import tkinter.font as tkfont

    win = tk.Toplevel()
    win.title(f"Structure Difference for {table_name}")
    frame = tk.Frame(win)
    frame.pack(expand=True, fill="both", padx=10, pady=10)

    # Fonts and colors
    default_font = tkfont.Font(family="Arial", size=10)
    diff_font = tkfont.Font(family="Arial", size=10, weight="bold")
    missing_font = tkfont.Font(family="Arial", size=10, slant="italic")

    # --- Columns ---
    tk.Label(frame, text="Source Table Columns", font=("Arial", 12, "bold")).grid(row=0, column=0, sticky="w", padx=5)
    tk.Label(frame, text="Target Table Columns", font=("Arial", 12, "bold")).grid(row=0, column=1, sticky="w", padx=5)

    src_dict = {col[0]: col for col in src_cols}
    tgt_dict = {col[0]: col for col in tgt_cols}
    all_col_names = sorted(set(src_dict.keys()) | set(tgt_dict.keys()))
    for i, col_name in enumerate(all_col_names):
        src_val = src_dict.get(col_name)
        tgt_val = tgt_dict.get(col_name)
        if src_val and tgt_val:
            if src_val == tgt_val:
                src_style = tgt_style = {"font": default_font, "fg": "black"}
            else:
                src_style = tgt_style = {"font": diff_font, "fg": "red"}
        elif src_val and not tgt_val:
            src_style = {"font": missing_font, "fg": "gray"}
            tgt_style = {"font": missing_font, "fg": "gray"}
        elif tgt_val and not src_val:
            src_style = {"font": missing_font, "fg": "gray"}
            tgt_style = {"font": missing_font, "fg": "gray"}
        else:
            src_style = tgt_style = {"font": default_font, "fg": "black"}

        src_text = str(src_val) if src_val else "(missing)"
        tgt_text = str(tgt_val) if tgt_val else "(missing)"
        tk.Label(frame, text=src_text, **src_style, anchor="w", justify="left").grid(row=i+1, column=0, sticky="w", padx=5)
        tk.Label(frame, text=tgt_text, **tgt_style, anchor="w", justify="left").grid(row=i+1, column=1, sticky="w", padx=5)

    # --- Constraints/Keys/Indices ---
    row_offset = len(all_col_names) + 2
    tk.Label(frame, text="Constraints & Indices", font=("Arial", 12, "bold")).grid(row=row_offset, column=0, columnspan=2, sticky="w", pady=(10,0))

    def format_dict(d):
        return "\n".join(f"{k}: {v}" for k, v in d.items()) if d else "(none)"

    # Primary Key
    pk_same = src_constraints["primary_key"] == tgt_constraints["primary_key"]
    pk_style = {"font": default_font, "fg": "black"} if pk_same else {"font": diff_font, "fg": "red"}
    tk.Label(frame, text=f"Primary Key: {src_constraints['primary_key']}", **pk_style, anchor="w", justify="left").grid(row=row_offset+1, column=0, sticky="w", padx=5)
    tk.Label(frame, text=f"Primary Key: {tgt_constraints['primary_key']}", **pk_style, anchor="w", justify="left").grid(row=row_offset+1, column=1, sticky="w", padx=5)

    # Unique Keys
    unique_same = src_constraints["unique_keys"] == tgt_constraints["unique_keys"]
    unique_style = {"font": default_font, "fg": "black"} if unique_same else {"font": diff_font, "fg": "red"}
    tk.Label(frame, text=f"Unique Keys:\n{format_dict(src_constraints['unique_keys'])}", **unique_style, anchor="w", justify="left").grid(row=row_offset+2, column=0, sticky="w", padx=5)
    tk.Label(frame, text=f"Unique Keys:\n{format_dict(tgt_constraints['unique_keys'])}", **unique_style, anchor="w", justify="left").grid(row=row_offset+2, column=1, sticky="w", padx=5)

    # Indices
    idx_same = src_constraints["indices"] == tgt_constraints["indices"]
    idx_style = {"font": default_font, "fg": "black"} if idx_same else {"font": diff_font, "fg": "red"}
    tk.Label(frame, text=f"Indices:\n{format_dict(src_constraints['indices'])}", **idx_style, anchor="w", justify="left").grid(row=row_offset+3, column=0, sticky="w", padx=5)
    tk.Label(frame, text=f"Indices:\n{format_dict(tgt_constraints['indices'])}", **idx_style, anchor="w", justify="left").grid(row=row_offset+3, column=1, sticky="w", padx=5)

    tk.Button(win, text="Close", command=win.destroy).pack(pady=5)

def show_result_table(result_rows, selected_tables, table_where_clauses):
    schema_frame.pack_forget()
    result_frame.pack(fill="both", expand=True)
    for widget in result_frame.winfo_children():
        widget.destroy()
    tk.Label(result_frame, text="Comparison Result", font=("Arial", 16, "bold")).pack(pady=10)
    columns = ("Table", "Exists in Target", "Structure", "Content", "Action")
    tree = ttk.Treeview(result_frame, columns=columns, show="headings", height=15)
    for col in columns:
        tree.heading(col, text=col)
        if col == "Exists in Target":
            tree.column(col, width=90)
        elif col == "Structure":
            tree.column(col, width=110)
        elif col == "Action":
            tree.column(col, width=220)
        else:
            tree.column(col, width=180)
    for row in result_rows:
        content = "-"
        if row[1] == "✅ Yes":
            src_count = tgt_count = None
            if isinstance(row[3], str):
                m = re.match(r".*src:\s*([0-9]+).*tgt:\s*([0-9]+)", row[3])
                if m:
                    src_count = m.group(1)
                    tgt_count = m.group(2)
            if row[3].startswith("✅ Same"):
                content = "Same"
            elif "Different" in row[3]:
                if src_count is not None and tgt_count is not None:
                    content = f"Different (src row: {src_count}, tgt row: {tgt_count})"
                else:
                    content = "Different"
            elif "Error" in row[3]:
                content = "Error"
            else:
                content = row[3]
        action = ""
        if row[1] == "✅ Yes" and (row[2] == "⚠️ Different" or "Different" in content):
            action = "🔗 Generate Upgrade Script"
        tree.insert("", tk.END, values=(row[0], row[1], row[2], content, action))
    tree.pack(padx=10, pady=10, fill="x")

    def on_tree_click(event):
        region = tree.identify("region", event.x, event.y)
        if region == "cell":
            col = tree.identify_column(event.x)
            row_id = tree.identify_row(event.y)
            if not row_id:
                return
            item = tree.item(row_id)
            table_name = item['values'][0]
            struct_status = item['values'][2]
            content_val = item['values'][3]
            action_val = item['values'][4]
            where_clause = table_where_clauses.get(table_name, None)
            if col == "#5":  # Action column
                if action_val.startswith("🔗"):
                    src_cols = get_table_columns(
                        db_connection,
                        source_db_var.get(), table_name
                    )
                    tgt_cols = get_table_columns(
                        target_connection,
                        target_db_var.get(), table_name
                    )
                    alter_sql = ""
                    data_sql = ""
                    if struct_status == "⚠️ Different":
                        alter_sql = generate_alter_table_sql(src_cols, tgt_cols, table_name)
                    if "Different" in content_val and struct_status == "✅ Same":
                        diff = compare_table_content(
                            db_connection, target_connection,
                            source_db_var.get(), target_db_var.get(), table_name,
                            source_where=where_clause, target_where=where_clause
                        )
                        if isinstance(diff, dict) and "error" not in diff:
                            # Identify auto-increment columns
                            auto_inc_cols = [src_col[0] for src_col in src_cols if "auto_increment" in src_col[5].lower()]
                            # When calling generate_content_sync_sql:
                            data_sql = generate_content_sync_sql(
                                diff["col_names"], diff["missing_in_target"], diff["missing_in_source"], table_name,
                                diff["values_different"], diff["pk"], auto_inc_cols=auto_inc_cols
                            )
                        else:
                            data_sql = "-- Error or structure not identical"
                    elif "Different" in content_val:
                        diff = compare_table_content(
                            db_connection,
                            source_db_var.get(), target_db_var.get(), table_name,
                            source_where=where_clause, target_where=where_clause
                        )
                        if isinstance(diff, dict) and "error" not in diff:
                            # Identify auto-increment columns
                            auto_inc_cols = [src_col[0] for src_col in src_cols if "auto_increment" in src_col[5].lower()]
                            # When calling generate_content_sync_sql:
                            data_sql = generate_content_sync_sql(
                                diff["col_names"], diff["missing_in_target"], diff["missing_in_source"], table_name,
                                diff["values_different"], diff["pk"], auto_inc_cols=auto_inc_cols
                            )
                        else:
                            data_sql = "-- Error or structure not identical"

                    script = "-- Upgrade Script\n"
                    if alter_sql:
                        script += f"\n-- Structure Upgrade\n{alter_sql}\n"
                    if data_sql:
                        script += f"\n-- Data Sync\n{data_sql}\n"
                    show_script_window(table_name, script)
                else:
                    messagebox.showinfo("No Upgrade Needed", "Selected table does not need an upgrade.")
            elif col == "#4":  # Content column
                if "Different" in content_val:
                    struct_status_val = item['values'][2]
                    if struct_status_val == "✅ Same":
                        try:
                            diff_json = compare_table_content(
                                db_connection, target_connection,
                                source_db_var.get(), target_db_var.get(), table_name,
                                source_where=where_clause, target_where=where_clause
                            )
                        except DbToolsError as e:
                            diff_json = {"error": str(e)}
                    else:
                        diff_json = {"error": "Structure is not identical, cannot compare content."}
                    show_content_diff_window(table_name, diff_json)
            elif col == "#3" and struct_status == "⚠️ Different":
                source_db = source_db_var.get()
                target_db = target_db_var.get()
                src_cols = get_table_columns(db_connection, source_db, table_name)
                tgt_cols = get_table_columns(db_connection, target_db, table_name)
                src_constraints = get_table_constraints_and_indices(db_connection, source_db, table_name)
                tgt_constraints = get_table_constraints_and_indices(db_connection, target_db, table_name)
                show_structure_diff_window(table_name, src_cols, tgt_cols, src_constraints, tgt_constraints)

    def on_tree_motion(event):
        region = tree.identify("region", event.x, event.y)
        col = tree.identify_column(event.x)
        row_id = tree.identify_row(event.y)
        if region == "cell" and row_id:
            item = tree.item(row_id)
            struct_status = item['values'][2]
            action_val = item['values'][4]
            content_val = item['values'][3]
            if (col == "#5" and action_val.startswith("🔗")) or \
               (col == "#4" and "Different" in content_val) or \
               (col == "#3" and struct_status == "⚠️ Different"):
                tree.config(cursor="hand2")
            else:
                tree.config(cursor="")
        else:
            tree.config(cursor="")

    tree.bind("<Button-1>", on_tree_click)
    tree.bind("<Motion>", on_tree_motion)

    tk.Button(result_frame, text="Back", command=lambda: back_to_schema(tree)).pack(pady=10)

def show_script_window(table_name, script):
    win = tk.Toplevel()
    win.title(f"Upgrade Script for {table_name}")
    script_window_width = int(700 * 1.3)
    script_window_height = 500
    screen_width = win.winfo_screenwidth()
    screen_height = win.winfo_screenheight()
    x = int((screen_width / 2) - (script_window_width / 2))
    y = int((screen_height / 2) - (script_window_height / 2))
    win.geometry(f"{script_window_width}x{script_window_height}+{x}+{y}")
    txt = tk.Text(win, wrap="word")
    txt.insert("1.0", script)
    txt.pack(expand=True, fill="both", padx=10, pady=10)
    tk.Button(win, text="Close", command=win.destroy).pack(pady=5)

def back_to_schema(tree_widget):
    result_frame.pack_forget()
    schema_frame.pack(fill="both", expand=True)
    tree_widget.destroy()

def update_where_clause_boxes(event=None):
    # Clear previous widgets
    for widget in where_clauses_frame.winfo_children():
        widget.destroy()
    selected_indices = source_tables_listbox.curselection()
    selected_tables = [source_tables_listbox.get(i) for i in selected_indices]
    for table in selected_tables:
        tk.Label(where_clauses_frame, text=f"WHERE Clause for '{table}':").pack(anchor="w", pady=(5, 0))
        text_widget = tk.Text(where_clauses_frame, width=90, height=5)
        text_widget.pack(pady=(0, 5))
        # Insert existing clause if present
        text_widget.insert("1.0", table_where_clauses.get(table, ""))
        # Closure to capture table name
        def save_clause(event, t=table, w=text_widget):
            table_where_clauses[t] = w.get("1.0", tk.END).strip()
        text_widget.bind("<KeyRelease>", save_clause)

def fill_fields_from_profile(event=None):
    profile = selected_profile_var.get()
    logging.info(f"Selected profile: {profile}")
    if profile and profile in connections:
        host_var.set(connections[profile].get("host", ""))
        port_var.set(connections[profile].get("port", ""))
        username_var.set(connections[profile].get("username", ""))
        password_var.set(connections[profile].get("password", ""))

def save_current_connection():
    profile = tk.simpledialog.askstring("Profile Name", "Enter a name for this connection profile:")
    if not profile:
        return
    connections[profile] = {
        "host": host_var.get(),
        "port": port_var.get(),
        "username": username_var.get(),
    }
    save_connections(connections)
    # Save password to keyring
    import keyring
    keyring.set_password("db-tools", f"{profile}_password", password_var.get())
    selected_profile_combo['values'] = list(connections.keys())
    selected_profile_combo.set(profile)
    messagebox.showinfo("Saved", f"Profile '{profile}' saved.")

def load_connection_frame():
    if profile_names:
        selected_profile_combo.set(profile_names[0])
        fill_fields_from_profile()
    selected_profile_combo.bind("<<ComboboxSelected>>", fill_fields_from_profile)

root = tk.Tk()
root.title("DB Tools - MySQL Connection")
window_width = int(800 * 1.3)
window_height = 900
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()
x = int((screen_width / 2) - (window_width / 2))
y = int((screen_height / 2) - (window_height / 2))
root.geometry(f"{window_width}x{window_height}+{x}+{y}")

# --- Connection Frame ---
connection_frame = tk.Frame(root)
connection_frame.pack(fill="both", expand=True)

tk.Label(connection_frame, text="Connection Profile:").pack(anchor="w", padx=10, pady=(10, 0))
selected_profile_var = tk.StringVar(value="default")
selected_profile_combo = ttk.Combobox(connection_frame, textvariable=selected_profile_var, values=profile_names, state="readonly", width=40)
selected_profile_combo.pack(padx=10, pady=2)
# if profile_names:
#     selected_profile_combo.set(profile_names[0])
#     fill_fields_from_profile()
# selected_profile_combo.bind("<<ComboboxSelected>>", fill_fields_from_profile)

tk.Label(connection_frame, text="MySQL Host:").pack(anchor="w", padx=10, pady=(10, 0))
host_var = tk.StringVar(value="127.0.0.1")
tk.Entry(connection_frame, textvariable=host_var, width=40).pack(padx=10, pady=2)

tk.Label(connection_frame, text="Port:").pack(anchor="w", padx=10, pady=(10, 0))
port_var = tk.StringVar(value="3306")
tk.Entry(connection_frame, textvariable=port_var, width=40).pack(padx=10, pady=2)

tk.Label(connection_frame, text="Username:").pack(anchor="w", padx=10, pady=(10, 0))
username_var = tk.StringVar(value="admin")
tk.Entry(connection_frame, textvariable=username_var, width=40).pack(padx=10, pady=2)

tk.Label(connection_frame, text="Password:").pack(anchor="w", padx=10, pady=(10, 0))
password_var = tk.StringVar(value="")
tk.Entry(connection_frame, textvariable=password_var, show="*", width=40).pack(padx=10, pady=2)

tk.Button(connection_frame, text="Save Connection Profile", command=save_current_connection).pack(pady=5)

# Target connection option
use_different_target_var = tk.BooleanVar()
tk.Checkbutton(connection_frame, text="Use different target connection", variable=use_different_target_var, command=lambda: toggle_target_connection()).pack(pady=5)

# Target connection frame
target_connection_frame = tk.Frame(connection_frame)

tk.Label(target_connection_frame, text="Target Profile:").pack(anchor="w", pady=(10, 0))
target_profile_var = tk.StringVar()
target_profile_combo = ttk.Combobox(target_connection_frame, textvariable=target_profile_var, values=profile_names, state="readonly", width=40)
target_profile_combo.pack(pady=2)

tk.Label(target_connection_frame, text="Target Host:").pack(anchor="w", pady=(10, 0))
target_host_var = tk.StringVar(value="127.0.0.1")
tk.Entry(target_connection_frame, textvariable=target_host_var, width=40).pack(pady=2)

tk.Label(target_connection_frame, text="Target Port:").pack(anchor="w", pady=(10, 0))
target_port_var = tk.StringVar(value="3306")
tk.Entry(target_connection_frame, textvariable=target_port_var, width=40).pack(pady=2)

tk.Label(target_connection_frame, text="Target Username:").pack(anchor="w", pady=(10, 0))
target_username_var = tk.StringVar(value="admin")
tk.Entry(target_connection_frame, textvariable=target_username_var, width=40).pack(pady=2)

tk.Label(target_connection_frame, text="Target Password:").pack(anchor="w", pady=(10, 0))
target_password_var = tk.StringVar(value="")
tk.Entry(target_connection_frame, textvariable=target_password_var, show="*", width=40).pack(pady=2)

def fill_target_fields_from_profile(event=None):
    profile = target_profile_var.get()
    if profile and profile in connections:
        target_host_var.set(connections[profile].get("host", ""))
        target_port_var.set(connections[profile].get("port", ""))
        target_username_var.set(connections[profile].get("username", ""))
        target_password_var.set(connections[profile].get("password", ""))

def toggle_target_connection():
    if use_different_target_var.get():
        target_connection_frame.pack(fill="x", padx=10, pady=5)
    else:
        target_connection_frame.pack_forget()

target_profile_combo.bind("<<ComboboxSelected>>", fill_target_fields_from_profile)

tk.Button(connection_frame, text="Connect", command=connect_and_next).pack(pady=20)

# --- Schema Frame ---
schema_frame = tk.Frame(root)

tk.Label(schema_frame, text="Available Databases:").pack(anchor="w", padx=10, pady=(10, 0))
db_list_var = tk.StringVar()
tk.Label(schema_frame, textvariable=db_list_var, justify="left", bg="#000000", anchor="nw", width=70, height=5).pack(padx=10, pady=5)

tk.Label(schema_frame, text="Select Source Database:").pack(anchor="w", padx=10, pady=(10, 0))
source_db_var = tk.StringVar()
source_db_combo = ttk.Combobox(schema_frame, textvariable=source_db_var, state="readonly", width=67)
source_db_combo.pack(padx=10, pady=2)
source_db_combo.bind("<<ComboboxSelected>>", update_source_tables)

tk.Label(schema_frame, text="Select Target Database:").pack(anchor="w", padx=10, pady=(10, 0))
target_db_var = tk.StringVar()
target_db_combo = ttk.Combobox(schema_frame, textvariable=target_db_var, state="readonly", width=67)
target_db_combo.pack(padx=10, pady=2)
target_db_combo.bind("<<ComboboxSelected>>", update_target_tables)

tk.Label(schema_frame, text="Source Tables (select one or more):").pack(anchor="w", padx=10, pady=(10, 0))
source_tables_listbox = tk.Listbox(schema_frame, selectmode=tk.MULTIPLE, width=70, height=10)
source_tables_listbox.pack(padx=10, pady=5)

# Store WHERE clauses for each table
table_where_clauses = {}
where_clause_text_widgets = {}

# Frame to hold dynamic WHERE clause widgets
where_clauses_frame = tk.Frame(schema_frame)
where_clauses_frame.pack(fill="x", padx=10, pady=2)


source_tables_listbox.bind("<<ListboxSelect>>", update_where_clause_boxes)

submit_btn = tk.Button(schema_frame, text="Submit", command=submit)
submit_btn.pack(pady=20)

# --- Result Frame ---
result_frame = tk.Frame(root)

def main():
    print("App started!", flush=True)
    load_connection_frame()
    root.mainloop()

if __name__ == "__main__":
    main()