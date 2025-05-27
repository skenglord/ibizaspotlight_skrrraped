#!/usr/bin/env python3
import subprocess
import sys
import os

def run_command(command, env=None):
    try:
        subprocess.run(command, check=True, env=env)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        sys.exit(1)

def main():
    if not os.path.exists("requirements.txt"):
        print("Error: requirements.txt not found in current directory")
        sys.exit(1)

    print("Starting installation process...")
    
    # Create virtual environment
    print("Creating virtual environment...")
    venv_dir = "env"
    venv_bin = os.path.join(venv_dir, "Scripts" if os.name == "nt" else "bin")
    run_command([sys.executable, "-m", "venv", venv_dir])
    
    # Get pip executable path
    pip_path = os.path.join(venv_bin, "pip.exe" if os.name == "nt" else "pip")
    if not os.path.exists(pip_path):
        print(f"Error: Pip executable not found at {pip_path}")
        sys.exit(1)
    
    # Upgrade pip
    print("Upgrading pip...")
    run_command([pip_path, "install", "--upgrade", "pip"])
    
    # Install Python dependencies
    print("Installing requirements from requirements.txt...")
    run_command([pip_path, "install", "-r", "requirements.txt"])
    
    # Final message
    activate_cmd = f"{venv_bin}\\activate" if os.name == "nt" else f"source {venv_bin}/activate"
    print("\nInstallation completed successfully!")
    print(f"\nTo activate environment: {activate_cmd}")

if __name__ == "__main__":
    main()
