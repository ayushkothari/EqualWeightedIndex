# Equal Weighted Index 
This project retrieves market data for S&P 500 companies, processes it to compute an equal-weighted index, and presents the results via an interactive dashboard

## Description
This project is designed to run a sequence of operations involving data fetching, index composition, and dashboard generation. It includes a test suite to ensure the correctness of the codebase and provides an automated script for executing the main process. The project leverages `pytest` for testing and Python for the main functionality.

### Assumption
Assume that all stocks that historically fell within the top 100 by market cap are also part of the S&P 500 today.
Using Outstanding shares as total shares data was not available

### Run the Script

The project comes with a `run.sh` script that will install the dependencies, run the tests, and then execute the main Python script (`main.py`) automatically. Here’s how you can use it:

#### Run the script without providing date arguments:
```bash
./run.sh
```
If no `--start-date` or `--end-date` is provided, the script will default to using today's date as the `end-date` and 30 days before today's date as the `start-date`.

#### Run the script with a specified date range:
```bash
./run.sh --start-date=2024-07-07 --end-date=2025-01-01
```
This will use the specified start and end dates for data processing.

#### Run the script with only a start date:
```bash
./run.sh --start-date=2024-07-07
```
This will use the specified start date and today's date as the `end-date`.

#### Run the script with only an end date:
```bash
./run.sh --end-date=2025-01-01
```
This will use the specified end date and 30 days before that as the `start-date`.

The script does the following:
1. Installs the necessary dependencies (if not already installed).
2. Runs the test suite using `pytest`.
3. If the tests pass, it executes the main script (`main.py`), which handles data fetching, index composition, and dashboard generation.

## Setup Instructions

To get started with this project, follow these steps:

### 1. Clone the repository

If you haven’t already cloned the repository, do so by running the following command:

```bash
git clone https://github.com/ayushkothari/EqualWeightedIndex.git
cd <repository-directory>
```

### 2. Install the dependencies

The project uses Python, and all the required dependencies are listed in the `requirements.txt` file. To install them, use the following command:

```bash
pip install -r requirements.txt
```

### 3. Create and activate a virtual environment (optional but recommended)

It’s recommended to create a virtual environment to keep dependencies isolated. You can do this by running:

```bash
python -m venv venv
source venv/bin/activate
```

### 4. Run Tests

To ensure the integrity of the codebase, the script first runs the test suite using `pytest`. To execute the tests, simply run the following command:

```bash
pytest
```

### 5. Run the Main Script

Once the tests have passed, you can run the main script with optional date arguments:

```bash
python main.py --start-date 2025-01-01 --end-date 2025-02-01
```

If no date arguments are provided, the script will run with default settings.

## Configuring the Number of Index Constituents

You can change the number of companies (constituents) in the index by modifying the `NO_OF_COMPANIES` constant in the `constants.py` file. 

To adjust the number of companies, open `constants.py` and update the following line:

```python
NO_OF_COMPANIES = 100  # Set this to any desired number of constituents
```

This allows you to dynamically create the index with a different number of companies, based on your preference.

## Project Flow
1. **Fetch stock market data**: The script retrieves historical stock prices and market capitalization data from Yahoo Finance.
2. **Store data in DuckDB**: The collected data is stored in a structured DuckDB database to enable efficient querying.
3. **Compute equal-weighted index**: Each of the top 100 companies is assigned an equal weight, and the overall index performance is calculated.
4. **Track index composition changes**: Daily shifts in the composition of the top 100 companies are logged.
5. **Generate reports and dashboards**: The data is visualized through an interactive dashboard with time-series analysis, company weights, and index trends.

## Future Improvements
- **Support for other stock indices**: Expand the project to include indices like NASDAQ-100 and Dow Jones Industrial Average.
- **Custom weighting methodologies**: Allow users to apply different weighting mechanisms (e.g., market-cap weighted, fundamental factors).
- **Integration with real-time stock data APIs**: Enhance data freshness by incorporating live market data feeds.
- **Advanced visualization features**: Improve the dashboard with additional analytics, historical trend comparisons, and interactive forecasting tools.
