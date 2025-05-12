import os
import subprocess

def activate_virtualenv():
    venv_path = os.path.join('venv', 'bin', 'activate')
    return venv_path

def run_script(script_path):
    try:
        activate_script = activate_virtualenv()
        command = f'source {activate_script} && python {script_path}'
        subprocess.check_call(['/bin/zsh', '-c', command])
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running the script: {e}")

def main():
    print("Welcome to cfkit! Please select a report to run:")

    options = {
        'cas_mapping': 'src/cas_mapping/main.py',
        'assessment_properties_tracker': 'src/assessment_properties_tracker/main.py',
        'assessment_tracker': 'src/assessment_tracker/main.py',
        'endpoint_report': 'src/endpoint_report/main.py',
        'h_statement_report': 'src/h_statement_report/main.py',
        'ingredient_intelligence_report': 'src/ingredient_intelligence_report/main.py',
        'small_tasks': 'src/small_tasks/main.py',
        'user_report': 'src/user_report/main.py',
        'visualization': 'src/visualization/main.py',
        'list_checking': 'src/list_checking/main.py'
    }

    print("1. Run CAS Mapping")
    print("2. Run Assessment Properties Tracker")
    print("3. Run Assessment Tracker")
    print("4. Run Endpoint Report")
    print("5. Run H Statement Report")
    print("6. Run Ingredient Intelligence Report")
    print("7. Run Small Tasks")
    print("8. Run User Report")
    print("9. Run Visualization")
    print("10. Run List Checking")

    choice = input("Enter the number of the report to run: ")

    if choice == '1':
        run_script(options['cas_mapping'])
    elif choice == '2':
        run_script(options['assessment_properties_tracker'])
    elif choice == '3':
        run_script(options['assessment_tracker'])
    elif choice == '4':
        run_script(options['endpoint_report'])
    elif choice == '5':
        run_script(options['h_statement_report'])
    elif choice == '6':
        run_script(options['ingredient_intelligence_report'])
    elif choice == '7':
        run_script(options['small_tasks'])
    elif choice == '8':
        run_script(options['user_report'])
    elif choice == '9':
        run_script(options['visualization'])
    elif choice == '10':
        run_script(options['list_checking'])
    else:
        print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()