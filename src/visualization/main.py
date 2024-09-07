import os
import subprocess

def run_script(script):
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "function_bubbles", script)
    print(f"Running {script}...")
    subprocess.run(["python3", script_path], check=True)
    print(f"Completed {script}.")

def main():
    print("Please select which script to run:")
    print("1. Functions Question Marks Null")
    print("2. Functions Question Marks Quarter")

    choice = input("Enter the number of the script to run: ")

    if choice == '1':
        run_script("functions_question_marks_null.py")
    elif choice == '2':
        run_script("functions_question_marks_quarter.py")
    else:
        print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
