# debug_main.py
# Now import and run your app
from .app import main
import debugpy
import sys
import os

# Add project root to Python path
sys.path.insert(0, os.getcwd())

print(f"🔧 CWD: {os.getcwd()}")
print(f"📄 __file__: {__file__}")

print("🟩 debugpy: Waiting for debugger to attach on port 5678...")
debugpy.listen(("0.0.0.0", 5678))
debugpy.wait_for_client()  # Pauses here until debugger attaches
print("🎉 Debugger attached! Loading tk app...")

if __name__ == "__main__":
    main()