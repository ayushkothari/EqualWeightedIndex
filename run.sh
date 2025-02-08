#!/bin/bash

# Run pytest first
echo "Running tests with pytest..."
pytest --maxfail=1 --disable-warnings -q

pip install -r requirements.txt
source venv/bin/activate

# Check if pytest passed
if [ $? -eq 0 ]; then
    echo "Tests passed successfully."
else
    echo "Tests failed. Aborting script."
    exit 1
fi

python main.py $1 $2