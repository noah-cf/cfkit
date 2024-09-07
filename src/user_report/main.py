import os
import subprocess

def run_script():
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "user_report.py")
    print(f"Running user_report.py...")
    subprocess.run(["python3", script_path], check=True)
    print(f"Completed user_report.py.")

if __name__ == "__main__":
    run_script()
