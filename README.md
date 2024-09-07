# Installing the cfkit

## Homebrew and Python Setup

### All the following lines can be simply copy and pasted if you wish, by copying everything
### except the "sh" between the two sets of ```.

1. Open a Terminal window

2. Install Homebrew (if not already installed)
    ```sh
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    ```

3. Install Python (if not already installed)
    ```sh
    brew install python
    ```

## Project Setup

1. Create the directory you wish to keep this project in.
    E.g., Recommendation:
    ~(Your User)/Developer/

2. Clone the repository and enter directory
    ```sh
    git clone https://github.com/noah-cf/cfkit.git
    cd cfkit
    ```

3. **Create a virtual environment**:
    ```sh
    python -m venv venv
    ```

4. **Activate the virtual environment**:
    ```sh
    source venv/bin/activate
    ```

5. **Install the required packages**:
    ```sh
    pip install --upgrade pip
    pip install -r requirements.txt
    ```

6. **Create a run_report.command shortcut**:
    ```sh
    echo '#!/bin/zsh\nsource "$(dirname "$0")/venv/bin/activate"\nexport PYTHONPATH=$PYTHONPATH:$(pwd)\npython "$(dirname "$0")/src/main.py"\ndeactivate' > cfkit_menu.command
    chmod +x cfkit_menu.command
    ```

### Running the Reports

1. Setting up input files
    

2. Running reports
    To run the cfkit, simply double-click the `cfkit_menu.command` file or run the following command in the terminal:
    ```sh
    ./cfkit_menu.command
    ```