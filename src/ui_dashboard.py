#!/usr/bin/env python3
import os
import subprocess
import sys

def main():
    """Launch the Streamlit UI dashboard"""
    # Get the path to the app.py file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    app_path = os.path.join(script_dir, "ui", "app.py")
    
    # Check if Streamlit is installed
    try:
        import streamlit
        print("Starting Trino Partitioning Dashboard...")
    except ImportError:
        print("Streamlit is not installed. Installing required packages...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit", "plotly", "matplotlib"])
        print("Packages installed successfully!")
    
    # Launch the Streamlit app
    cmd = [sys.executable, "-m", "streamlit", "run", app_path, "--server.port=8501"]
    print(f"Running command: {' '.join(cmd)}")
    subprocess.run(cmd)

if __name__ == "__main__":
    main() 