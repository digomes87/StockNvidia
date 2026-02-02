
import os
import subprocess
import sys
import venv
from pathlib import Path

def create_structure():
    """Create project directory structure"""
    directories = [
        "config",
        "data",
        "notebooks",
        "output",
        "src",
        "tests",
        "logs"
    ]
    
    print("Creating directory structure...")
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"  Created/Verified: {directory}/")

def create_requirements():
    """Create requirements.txt if not exists"""
    requirements = """pyspark
pandas
matplotlib
seaborn
pyyaml
kagglehub
numpy
python-dotenv
pyarrow
"""
    if not os.path.exists("requirements.txt"):
        print("Creating requirements.txt...")
        with open("requirements.txt", "w") as f:
            f.write(requirements)
        print("  Created: requirements.txt")
    else:
        print("  Verified: requirements.txt")

def create_env_file():
    """Create .env file if not exists"""
    env_content = """# Kaggle Credentials
KAGGLE_USERNAME=
KAGGLE_KEY=

# Spark Configuration
SPARK_APP_NAME=NVIDIA_Stock_Analysis
SPARK_MASTER=local[*]
"""
    if not os.path.exists(".env"):
        print("Creating .env file...")
        with open(".env", "w") as f:
            f.write(env_content)
        print("  Created: .env")
    else:
        print("  Verified: .env")

def create_gitignore():
    """Create .gitignore if not exists"""
    gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual Environment
.env
.venv
env/
venv/
ENV/

# Spark
spark-warehouse/
metastore_db/
derby.log

# IDEs
.idea/
.vscode/
*.swp
*.swo

# Project specific
data/
output/
logs/
.DS_Store
"""
    if not os.path.exists(".gitignore"):
        print("Creating .gitignore...")
        with open(".gitignore", "w") as f:
            f.write(gitignore_content)
        print("  Created: .gitignore")
    else:
        print("  Verified: .gitignore")

def setup_environment():
    """Setup virtual environment using uv if available, else venv"""
    print("\nSetting up environment...")
    
    # Check if uv is installed
    try:
        subprocess.run(["uv", "--version"], check=True, capture_output=True)
        has_uv = True
        print("  Detected uv package manager")
    except (subprocess.CalledProcessError, FileNotFoundError):
        has_uv = False
        print("  uv not found, using standard venv")

    if has_uv:
        if not os.path.exists(".venv"):
            print("  Creating venv with uv...")
            subprocess.run(["uv", "venv", ".venv"], check=True)
        
        print("  Installing dependencies with uv...")
        # Ensure pip is installed in the venv
        subprocess.run(["uv", "pip", "install", "pip"], check=True)
        subprocess.run(["uv", "pip", "install", "-r", "requirements.txt"], check=True)
    else:
        if not os.path.exists(".venv"):
            print("  Creating venv...")
            venv.create(".venv", with_pip=True)
        
        print("  Installing dependencies...")
        pip_cmd = os.path.join(".venv", "bin", "pip")
        if sys.platform == "win32":
            pip_cmd = os.path.join(".venv", "Scripts", "pip")
            
        subprocess.run([pip_cmd, "install", "--upgrade", "pip"], check=True)
        subprocess.run([pip_cmd, "install", "-r", "requirements.txt"], check=True)

def main():
    print("="*50)
    print("Starting Project Setup")
    print("="*50)
    
    create_structure()
    create_requirements()
    create_env_file()
    create_gitignore()
    setup_environment()
    
    print("\n" + "="*50)
    print("Setup Completed Successfully!")
    print("="*50)
    print("\nTo activate the environment:")
    print("  source .venv/bin/activate")

if __name__ == "__main__":
    main()
