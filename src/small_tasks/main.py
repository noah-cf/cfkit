import os
import subprocess

def run_script(script):
    script_path = os.path.join(os.path.dirname(__file__), "scripts", script)
    print(f"Running {script}...")
    subprocess.run(["python3", script_path], check=True)
    print(f"Completed {script}.")

def main():
    print("Please select which script to run:")
    print("1. Merge with Delimiter")
    print("2. Split by Delimiter")
    print("3. Split Excel into Equal Parts")

    choice = input("Enter the number of the script to run: ")

    if choice == '1':
        run_script("merge_with_delimiter.py")
    elif choice == '2':
        run_script("split_by_delimiter.py")
    elif choice == '3':
        run_script("excel_splitter.py")
    else:
        print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()