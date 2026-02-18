# db-tools

## Overview

`db-tools` is a Python project that provides a set of utilities for interacting with MySQL databases. It offers both a desktop GUI application and a web application for comparing and synchronizing database schemas and content.

## Features

*   **Connection Management:** Save and load database connection profiles.
*   **Database Comparison:**
    *   Compare table structures (columns, primary keys, unique keys, indices).
    *   Compare table content (row counts and data differences).
*   **Script Generation:**
    *   Generates `ALTER TABLE` SQL to synchronize table structures.
    *   Generates `INSERT`, `UPDATE`, and `DELETE` SQL to synchronize table content.
*   **User Interfaces:**
    *   A `tkinter`-based desktop GUI.
    *   A `streamlit`-based web application.
*   **Debugging Support:** Includes wrapper scripts to facilitate debugging.

## Installation

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/your-username/db-tools.git
    cd db-tools
    ```

2.  **Install the dependencies:**

    The project uses `uv` for dependency management. You can install the dependencies with the following command:

    ```bash
    uv pip install -e .
    ```

    Alternatively, you can install the dependencies using `pip`:

    ```bash
    pip install -e .
    ```

## Usage

### Desktop Application

To run the `tkinter`-based desktop application, use the following command:

```bash
db-tools
```

### Web Application

To run the `streamlit`-based web application, use the following command:

```bash
streamlit run src/db_tools/web_app.py
```

### Debugging

The project includes wrapper scripts for debugging both the desktop and web applications.

*   **Desktop Application:**

    To run the desktop application with the debugger, run the `debug` script:

    ```bash
    debug
    ```

*   **Web Application:**

    To run the web application with the debugger, run the `web-debug` script:

    ```bash
    web-debug
    ```

    You can then attach your debugger to port 5678.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License.
