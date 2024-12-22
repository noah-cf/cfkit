# Installing cfkit

## Homebrew and Python Setup

### All the following lines can be simply copy and pasted if you wish, by copying everything
### except the "sh" between the two sets of ```.

1. Open a Terminal window.

2. Install Homebrew (if not already installed):
    ```sh
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    ```

3. Install Python (if not already installed):
    ```sh
    brew install python
    ```

## Project Setup

1. Create the directory you wish to keep this project in.
    E.g., Recommendation:
    ~/Developer/

2. Clone the repository and enter the directory:
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

5. **Ensure pip, setuptools, and wheel are up-to-date**:
    ```sh
    pip install --upgrade pip setuptools wheel
    ```

6. **Install the project (editable mode)**:
    ```sh
    pip install -e .
    ```

7. **Create a run_report.command shortcut**:
    ```sh
    echo '#!/bin/zsh\ncd "$(dirname "$0")"\nsource venv/bin/activate\nexport PYTHONPATH=$PYTHONPATH:$(pwd)/src\npython main.py\ndeactivate' > cfkit_menu.command
    chmod +x cfkit_menu.command
    ```

### Running the Reports

1. Setting up input files.

2. Running reports:
    To run cfkit, simply double-click the `cfkit_menu.command` file or run the following command in the terminal:
    ```sh
    ./cfkit_menu.command
    ```
