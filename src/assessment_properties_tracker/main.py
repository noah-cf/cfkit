import os
import subprocess

def run_script():
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "assessment_properties_tracker.py")
    print(f"Running assessment_properties_tracker.py...")
    subprocess.run(["python3", script_path], check=True)
    print(f"Completed assessment_properties_tracker.py.")

if __name__ == "__main__":
    run_script()
