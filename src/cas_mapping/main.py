import os
import subprocess

def run_script(script):
    script_path = os.path.join(os.path.dirname(__file__), "scripts", script)
    print(f"Running {script}...")
    subprocess.run(["python3", script_path], check=True)
    print(f"Completed {script}.")

def run_all_scripts():
    scripts = [ "cas_mapping.py"]
    for script in scripts:
        run_script(script)

if __name__ == "__main__":
    run_all_scripts()
