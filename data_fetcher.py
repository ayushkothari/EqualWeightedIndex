import duckdb
import yfinance as yf
import pandas as pd
import numpy as np
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from constants import CREATE_SCHEMA_SQL, INSERT_COMPANY_DATA_SQL, INSERT_MARKET_DATA_SQL, SP500_TICKERS, DB_PATH

# Configure logging to display messages in the terminal only
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(message)s'))  # Only show raw messages
logger.addHandler(handler)


def calculate_adjusted_shares(shares_outstanding: float, splits: pd.Series, hist_index: pd.DatetimeIndex) -> pd.Series:
    """Adjusts shares outstanding based on stock split history."""
    if splits.empty:
        return pd.Series(shares_outstanding, index=hist_index)

    # Sort splits in ascending order to process them correctly
    splits_ascending = splits.sort_index(ascending=True)
    split_dates = splits_ascending.index
    split_ratios = splits_ascending.values

    # Reverse the split ratios and compute cumulative product in reverse order
    reverse_ratios = split_ratios[::-1]
    cumprod_reverse = np.cumprod(reverse_ratios)[::-1]
    split_products = pd.Series(cumprod_reverse, index=split_dates)

    # Find the applicable split adjustment for each historical date
    hist_dates_np = hist_index.to_numpy()
    split_dates_np = split_dates.to_numpy()
    indices = np.searchsorted(split_dates_np, hist_dates_np, side='right')

    product = [split_products.iloc[indices[i]] if indices[i] < len(split_dates_np) else 1
               for i in range(len(hist_dates_np))]

    return shares_outstanding / np.array(product)


def fetch_ticker_data(ticker: str, start_date: datetime, end_date: datetime) -> tuple:
    """Fetches historical market data and calculates market capitalization."""
    try:
        stock = yf.Ticker(ticker)
        # Retrieve historical closing prices
        hist = stock.history(start=start_date, end=end_date, interval="1d")["Close"]
        shares_outstanding = stock.info.get("sharesOutstanding", None)
        company_name = stock.info.get("longName", ticker)

        # Custom error for missing shares data
        if not shares_outstanding:
            logger.error(f"MISSING DATA ERROR: No shares outstanding data for {ticker}")
            return ticker, company_name, pd.Series(dtype='float64'), pd.Series(dtype='float64')

        # Adjust shares for stock splits
        splits = stock.splits
        adjusted_shares = calculate_adjusted_shares(shares_outstanding, splits, hist.index)
        market_caps = (hist * adjusted_shares).astype("int64")

        return ticker, company_name, hist, market_caps
    except Exception as e:
        logger.error(f"FETCH ERROR: {ticker} - {str(e)}")
        return ticker, ticker, pd.Series(dtype='float64'), pd.Series(dtype='float64')


def create_database_schema(conn):
    """Creates the necessary database schema in DuckDB."""
    try:
        conn.execute(CREATE_SCHEMA_SQL)
        logger.info("Schema created successfully")
    except Exception as e:
        logger.error(f"SCHEMA CREATION ERROR: {str(e)}")


def insert_company_data(conn, ticker: str, company_name: str):
    """Inserts company data into the companies table, ignoring duplicates."""
    try:
        conn.execute(INSERT_COMPANY_DATA_SQL, [ticker, company_name])
    except Exception as e:
        logger.error(f"DB INSERT ERROR: Company data for {ticker} - {str(e)}")


def insert_market_data(conn, df: pd.DataFrame):
    """Inserts market data into the market_data table using a temporary DataFrame."""
    try:
        conn.register('temp_df', df)
        conn.execute(INSERT_MARKET_DATA_SQL)
    except Exception as e:
        logger.error(f"DB INSERT ERROR: Market data - {str(e)}")


def main(start_date=None, end_date=datetime.today()):
    """Main function that initializes the database, fetches data, and stores it."""
    conn = duckdb.connect(DB_PATH)
    # conn = duckdb.connect(':memory:')  # In memory
    if not start_date:
        start_date = end_date - timedelta(days=30)
    logger.info(f'start_date is {start_date} and end_date is {end_date}')
    try:
        create_database_schema(conn)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(fetch_ticker_data, t, start_date, end_date)
                       for t in SP500_TICKERS]

            for future in as_completed(futures):
                ticker, company_name, hist, market_caps = future.result()
                if not hist.empty and not market_caps.empty:
                    insert_company_data(conn, ticker, company_name)

                    df = pd.DataFrame({
                        'date': hist.index.date,
                        'ticker': ticker,
                        'close_price': hist.values,
                        'market_cap': market_caps.values
                    })
                    insert_market_data(conn, df)

        logger.info(f"\nData successfully saved to {DB_PATH}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()