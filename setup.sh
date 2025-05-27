#!/bin/bash
set -e

echo "Starting setup process..."

# Verify requirements file exists
if [ ! -f requirements.txt ]; then
    echo "Error: requirements.txt not found in current directory"
    exit 1
fi

# Create virtual environment
echo "Creating virtual environment..."
python -m venv env

# Activate environment
source env/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Final instructions
echo "Setup completed successfully!"
echo "To activate environment: source env/bin/activate"
echo "To deactivate: deactivate"
