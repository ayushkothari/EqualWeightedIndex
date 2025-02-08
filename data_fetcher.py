import duckdb
import yfinance as yf
import pandas as pd
import logging
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from datetime import datetime, timedelta
from constants import CREATE_SCHEMA_SQL, INSERT_COMPANY_DATA_SQL, INSERT_MARKET_DATA_SQL, SP500_TICKERS, DB_PATH

# Configure logging to display messages in the terminal only
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(message)s'))  # Only show raw messages
logger.addHandler(handler)


def fetch_ticker_data(ticker: str, start_date: datetime, end_date: datetime) -> Tuple[str, str, pd.Series, pd.Series]:
    """Fetches historical market data and calculates market capitalization."""
    try:
        stock = yf.Ticker(ticker)
        # Retrieve historical closing prices
        hist = stock.history(start=start_date, end=end_date, interval="1d")['Close']
        shares_outstanding = stock.info.get("sharesOutstanding", None)
        company_name = stock.info.get("longName", ticker)

        # Custom error for missing shares data
        if not shares_outstanding:
            logger.error(f"MISSING DATA ERROR: No shares outstanding data for {ticker}")
            return ticker, company_name, pd.Series(dtype='float64'), pd.Series(dtype='float64')

        market_caps = (hist * shares_outstanding).astype("int64")

        return ticker, company_name, hist, market_caps
    except Exception as e:
        logger.error(f"FETCH ERROR: {ticker} - {str(e)}")
        return ticker, ticker, pd.Series(dtype='float64'), pd.Series(dtype='float64')


def create_database_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Creates the necessary database schema in DuckDB."""
    try:
        conn.execute(CREATE_SCHEMA_SQL)
        logger.info("Schema created successfully")
    except Exception as e:
        logger.error(f"SCHEMA CREATION ERROR: {str(e)}")


def insert_company_data(conn: duckdb.DuckDBPyConnection, ticker: str, company_name: str) -> None:
    """Inserts company data into the companies table, ignoring duplicates."""
    try:
        conn.execute(INSERT_COMPANY_DATA_SQL, [ticker, company_name])
    except Exception as e:
        logger.error(f"DB INSERT ERROR: Company data for {ticker} - {str(e)}")


def insert_market_data(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """Inserts market data into the market_data table using a temporary DataFrame."""
    try:
        conn.register('temp_df', df)
        conn.execute(INSERT_MARKET_DATA_SQL)
    except Exception as e:
        logger.error(f"DB INSERT ERROR: Market data - {str(e)}")


def main(start_date: datetime = None, end_date: datetime = datetime.today()) -> None:
    """Main function that initializes the database, fetches data, and stores it."""
    conn = duckdb.connect(DB_PATH)
    # conn = duckdb.connect(':memory:')  # In memory
    if not start_date:
        start_date = end_date - timedelta(days=30)
    logger.info(f'start_date is {start_date} and end_date is {end_date}')
    try:
        create_database_schema(conn)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures: list[Future] = [executor.submit(fetch_ticker_data, t, start_date, end_date)
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
