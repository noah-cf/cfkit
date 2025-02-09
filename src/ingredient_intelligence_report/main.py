import os
import shutil
import subprocess
import time

script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(script_dir, '../../'))

file_paths = {
    "01_cas_cleaning": {
        "input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/01_cas_cleaning_input.xlsx'),
        "output": os.path.join(root_dir, 'data/ingredient_intelligence_reports/01_cas_cleaning_output.xlsx'),
        "next_input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/02_fetch_additional_casrns_input.xlsx')
    },
    "02_fetch_additional_casrns": {
        "input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/02_fetch_additional_casrns_input.xlsx'),
        "output": os.path.join(root_dir, 'data/ingredient_intelligence_reports/02_fetch_additional_casrns_output.xlsx'),
        "next_input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/03_fetch_data_cf_input.xlsx')
    },
    "03_fetch_data_cf": {
        "input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/03_fetch_data_cf_input.xlsx'),
        "output": os.path.join(root_dir, 'data/ingredient_intelligence_reports/03_fetch_data_cf_output.xlsx'),
        "next_input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/04_fetch_data_pharos_input.xlsx')
    },
    "04_fetch_data_pharos": {
        "input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/04_fetch_data_pharos_input.xlsx'),
        "output": os.path.join(root_dir, 'data/ingredient_intelligence_reports/04_fetch_data_pharos_output.xlsx'),
        "next_input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/05_gathered_data_merge_input.xlsx')
    },
    "05_gathered_data_merge": {
        "input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/05_gathered_data_merge_input.xlsx'),
        "output": os.path.join(root_dir, 'data/ingredient_intelligence_reports/05_gathered_data_merge_output.xlsx'),
        "next_input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/06_gathered_data_report_input.xlsx')
    },
    "06_gathered_data_report": {
        "input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/06_gathered_data_report_input.xlsx'),
        "output": os.path.join(root_dir, 'data/ingredient_intelligence_reports/06_gathered_data_report_output.xlsx'),
        "next_input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/07_report_breakdown_input.xlsx')
    },
    "07_report_breakdown": {
        "input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/07_report_breakdown_input.xlsx'),
        "output": os.path.join(root_dir, 'data/ingredient_intelligence_reports/07_report_breakdown_output.xlsx'),
        "next_input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/08_report_formatting_input.xlsx')
    },
    "08_report_formatting": {
        "input": os.path.join(root_dir, 'data/ingredient_intelligence_reports/08_report_formatting_input.xlsx'),
        "output": os.path.join(root_dir, 'data/ingredient_intelligence_reports/08_report_formatting_output.xlsx')
    }
}

def run_script(script_name):
    """
    Runs a specified script and handles output copying if necessary.
    
    :param script_name: Name of the script to run (without the .py extension)
    :type script_name: str
    """
    script_path = os.path.join(script_dir, 'scripts', f"{script_name}.py")
    print(f"Running {script_name}.py...")
    subprocess.run(["python3", script_path], check=True)
    print(f"Completed {script_name}.py.")
    time.sleep(2)

    if 'next_input' in file_paths[script_name]:
        output = file_paths[script_name]['output']
        next_input = file_paths[script_name]['next_input']
        print(f"Checking if {output} exists...")
        if os.path.exists(output):
            shutil.copy2(output, next_input)
            print(f"Copied and renamed {output} to {next_input}.")
        else:
            print(f"File {output} does not exist. Copy operation skipped.")

def run_all_scripts():
    """
    Runs all scripts in sequence as specified.
    """
    scripts = [
        '01_cas_cleaning',
        '02_fetch_additional_casrns',
        '03_fetch_data_cf',
        '04_fetch_data_pharos',
        '05_gathered_data_merge',
        '06_gathered_data_report',
        '07_report_breakdown',
        '08_report_formatting'
    ]
    for script in scripts:
        run_script(script)
    print("Completed all scripts.")

if __name__ == "__main__":
    run_all_scripts()
    