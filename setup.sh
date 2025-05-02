#!/bin/bash

# setup.sh
echo "Setting up project virtual environment..."

# Install venv if needed
sudo apt update
sudo apt install python3 python3-venv -y

# Create and activate venv
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

echo " Setup complete"
