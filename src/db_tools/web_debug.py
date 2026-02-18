import debugpy
import sys
import os

# Enable debugpy before importing streamlit
print("🟩 debugpy: Waiting for debugger to attach on port 5678...")
debugpy.listen(("0.0.0.0", 5678))
debugpy.wait_for_client()
print("🎉 Debugger attached! Loading Streamlit app...")

# Now import and run streamlit
import streamlit.web.cli as stcli

def main():
    sys.argv = ["streamlit", "run", "src/db_tools/web_app.py", "--logger.level=debug"]
    stcli.main()

if __name__ == "__main__":
    main()