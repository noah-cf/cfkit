import os
import subprocess

def run_script(script):
    script_path = os.path.join(os.path.dirname(__file__), "scripts", script)
    print(f"Running {script}...")
    subprocess.run(["python3", script_path], check=True)
    print(f"Completed {script}.")

def main():
    print("Please select which list checking script to run:")
    print("1. Report List Check")

    choice = input("Enter the number of the script to run: ")

    if choice == '1':
        run_script("report_list_check.py")
    else:
        print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()